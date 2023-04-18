package gr.aueb.dmst.nickkatsios;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Reducer;

// TODO: replace output.collect with context.write
// TODO: replace the input and output classes since we are dealing with (x,y) coordinates
//  represented as Point2D.Double objects and not single double values
// TODO: Simplify initial argument files
// TODO: Decide sorting of Point2D old and new centers to match them in the comparison: https://stackoverflow.com/questions/29030348/collections-sort-doesnt-work-on-listpoint2d-double

@SuppressWarnings("deprecation")
public class Kmeans {
    public static String OUT = "outfile";
    public static String IN = "inputlarger";
    public static String CENTROID_FILE_NAME = "/centroid.txt";
    public static String OUTPUT_FILE_NAME = "/part-00000";
    public static String DATA_FILE_NAME = "/data.txt";
    public static String JOB_NAME = "KMeans";

    public static double CONVERGENCE_THRESHOLD = 0.01;
    public static List<Point2D.Double> mCenters = new ArrayList<Point2D.Double>();

    /*
     * In Mapper class we are overriding configure function. In this we are
     * reading file from Distributed Cache and then storing that into instance
     * variable "mCenters"
     */
    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {

        // configure runs once before the whole mapping process and for each job
        // clears the old centroids and substitutes them with the new ones , located in
        // the file in the distributed cache
        @Override
        public void configure(JobConf job) {
            try {
                // Fetch the file from Distributed Cache Read it and store the
                // centroid in the ArrayList
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    mCenters.clear();
                    BufferedReader cacheReader = new BufferedReader(
                            new FileReader(cacheFiles[0].toString()));
                    try {
                        // Read the file split by the splitter and store it in
                        // the list
                        while ((line = cacheReader.readLine()) != null) {
                            String[] parts = line.split(" ");
                            Point2D.Double center = new Point2D.Double(Double.parseDouble(parts[0]) , Double.parseDouble(parts[1]));
                            mCenters.add(center);
                        }
                    } finally {
                        cacheReader.close();
                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistribtuedCache: " + e);
            }
        }

        /*
         * Map function will find the minimum center of the point and emit it to
         * the reducer
         */
        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<DoubleWritable, DoubleWritable> output,
                        Reporter reporter) throws IOException {
            // read the values from the data file
            String line = value.toString();
            String[] parts = line.split(" ");
            Point2D.Double point = new Point2D.Double(Double.parseDouble(parts[0]) , Double.parseDouble(parts[1]));
            double minDist = Double.MAX_VALUE;
            Point2D.Double nearest_center = mCenters.get(0);
            // Find the minimum center from a point
            for (Point2D.Double center : mCenters) {
                double dist = Point2D.distance(point.getX() , point.getY() , center.getX() , center.getY());
                if (dist < minDist) {
                    nearest_center = center;
                    minDist = dist;
                }
            }
            // Emit the nearest center and the point
            //output.collect(new DoubleWritable(nearest_center),
            //        new DoubleWritable(point));
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        /*
         * Reduce function will emit all the points to that center and calculate
         * the next center for these points
         */
        // reduce runs once for every key in the mapped key-value pairs
        @Override
        public void reduce(DoubleWritable key, Iterator<Text> values,
                           OutputCollector<DoubleWritable, Text> output, Reporter reporter)
                throws IOException {

            double sumX = 0.0;
            double sumY = 0.0;
            int count = 0;
            for (Iterator<Text> it = values; it.hasNext(); ) {
                Text value = it.next();
                String[] parts = value.toString().split(" ");
                if (parts.length == 2) {
                    Point2D.Double point = new Point2D.Double(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
                    sumX += point.getX();
                    sumY += point.getY();
                    count++;
                }
            }
            Point2D.Double newCenter = new Point2D.Double(sumX / count, sumY / count);

            // Emit new center and point
            //output.collect(new DoubleWritable(newCenter), new Text(points));
        }
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        IN = args[0];
        OUT = args[1];
        String input = IN;
        String output = OUT + System.nanoTime();
        String again_input = output;

        // Reiterating till the convergence
        int iteration = 0;
        boolean isdone = false;
        while (isdone == false) {
            JobConf conf = new JobConf(Kmeans.class);
            if (iteration == 0) {
                Path hdfsPath = new Path(input + CENTROID_FILE_NAME);
                // upload the file to hdfs. Overwrite any existing copy.
                DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
            } else {
                Path hdfsPath = new Path(again_input + OUTPUT_FILE_NAME);
                // upload the file to hdfs. Overwrite any existing copy.
                DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
            }

            conf.setJobName(JOB_NAME);
            conf.setMapOutputKeyClass(DoubleWritable.class);
            conf.setMapOutputValueClass(DoubleWritable.class);
            conf.setOutputKeyClass(DoubleWritable.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf,
                    new Path(input + DATA_FILE_NAME));
            FileOutputFormat.setOutputPath(conf, new Path(output));

            JobClient.runJob(conf);

            Path ofile = new Path(output + OUTPUT_FILE_NAME);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    fs.open(ofile)));
            List<Point2D.Double> centers_next = new ArrayList<Point2D.Double>();
            String line = br.readLine();
            while (line != null) {
                String[] parts = line.split(" ");
                Point2D.Double center = new Point2D.Double(Double.parseDouble(parts[0]) , Double.parseDouble(parts[1]));
                centers_next.add(center);
                line = br.readLine();
            }
            br.close();

            String prev;
            if (iteration == 0) {
                prev = input + CENTROID_FILE_NAME;
            } else {
                prev = again_input + OUTPUT_FILE_NAME;
            }
            Path prevfile = new Path(prev);
            FileSystem fs1 = FileSystem.get(new Configuration());
            BufferedReader br1 = new BufferedReader(new InputStreamReader(
                    fs1.open(prevfile)));
            List<Point2D.Double> centers_prev = new ArrayList<Point2D.Double>();
            line = br1.readLine();
            while (line != null) {
                String[] parts = line.split(" ");
                Point2D.Double center = new Point2D.Double(Double.parseDouble(parts[0]) , Double.parseDouble(parts[1]));
                centers_prev.add(center);
                line = br1.readLine();
            }
            br1.close();

            // Sort the old centroid and new centroid and check for convergence condition
            //we sort them to check if they are close enough one by one
            Collections.sort(centers_next);
            Collections.sort(centers_prev);

            Iterator<Point2D.Double> it = centers_prev.iterator();

            for (Point2D.Double old_center : centers_next) {
                Point2D.Double new_center = it.next();
                // got the 2 centers
                // now check if they are close enough
                double dist = Point2D.distance(old_center.getX() , old_center.getY() , new_center.getX() , new_center.getY());
                if (dist <= CONVERGENCE_THRESHOLD) {
                    isdone = true;
                } else {
                    isdone = false;
                    break;
                }
            }
            ++iteration;
            again_input = output;
            output = OUT + System.nanoTime();
        }
    }
}
