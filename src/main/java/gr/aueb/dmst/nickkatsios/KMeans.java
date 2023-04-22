package gr.aueb.dmst.nickkatsios;

import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

@SuppressWarnings("deprecation")
public class KMeans {

    public static String OUT = "outfile";
    public static String IN = "inputlarger";

    // the threshold set for convergence between old and new centers
    public static final double CONVERGENCE_THRESHOLD = 0.1;
    // the name of the standard output file name in /outputNanoTime/ output folder
    public static String OUTPUT_FILE_NAME = "/part-r-00000";
    // the name of the initial file containing the centroids
    public static String CENTROID_FILE_NAME = "/centroid.txt";
    // the name of the initial file containing the data
    public static String DATA_FILE_NAME = "/data.txt";

    // utility method to calculate euclidean distance between 2 points
    public static double calculateDistanceBetweenPoints(
            double x1,
            double y1,
            double x2,
            double y2) {
        return Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
    }

    // KEYIN: LongWritable iteration of the data file line by line
    // VALUEIN: Text the line in the data file
    // KEYOUT: PointWritable for reducing key comparison
    // VALUEOUT: PointWritable for assigning a point to the center
    public static class KMeansMapper extends Mapper<LongWritable, Text, PointWritable, PointWritable> {
        private ArrayList<PointWritable> centers = new ArrayList<>();
        private final PointWritable centroid = new PointWritable();

        // Before every mapping , load the current centroids from the distributed cache and store
        // them in an array of centers
        // setup is called before every mapping
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            try {
                // Fetch the file from Distributed Cache Read it and store the
                // centroid in the ArrayList
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    centers.clear();
                    BufferedReader cacheReader = new BufferedReader(
                            new FileReader(cacheFiles[0].toString()));
                    try {
                        // Read the file split by the splitter and store it in
                        // the list
                        while ((line = cacheReader.readLine()) != null) {
                            String[] parts = line.split(" ");
                            PointWritable center = new PointWritable(Double.parseDouble(parts[0]) , Double.parseDouble(parts[1]));
                            centers.add(center);
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
         * LongWritable because When you read a file with a M/R program,
         * the input key of your mapper should be the index of the line in the file,
         * while the input value will be the full line.
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input point
            String[] parts = value.toString().split(" ");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);
            PointWritable point = new PointWritable(x, y);

            // Find the nearest centroid
            double minDistance = Double.MAX_VALUE;
            PointWritable nearest = null;
            for (PointWritable center : centers) {
                double distance = calculateDistanceBetweenPoints(point.getX() , point.getY() , center.getX() ,center.getY());
                if (distance < minDistance) {
                    minDistance = distance;
                    nearest = center;
                }
            }

            // Emit the nearest centroid and the point
            context.write(nearest, point);
        }
    }

    // KEYIN: PointWritable nearest center emited by mapper
    // VALUEIN: Iterable<PointWritable> points emited by mapper corresponding to that specific center
    // KEYOUT: PointWritable for reducing key comparison
    // VALUEOUT: PointWritable for assigning a point to the center
    public static class KMeansReducer extends Reducer<PointWritable, PointWritable, Text, NullWritable> {

        /*
         * Reduce function will emit all the points to that center and calculate
         * the next center for these points
         * Reduce runs once for every key in the mapped key-value pairs
         */
        @Override
        public void reduce(PointWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
            // Compute the new centroid as the mean of the points
            int count = 0;
            double sumX = 0;
            double sumY = 0;
            for (PointWritable point : values) {
                count++;
                sumX += point.getX();
                sumY += point.getY();
            }
            PointWritable centroid = new PointWritable(sumX / count, sumY / count);
            Text centroidtText = new Text(centroid.getX() + " " + centroid.getY());
            // Emit the new centroid to the final output file located in outputNanoTime folder
            context.write(centroidtText, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        // args[0] = path to data.txt
        // args[1] = path to output folder (if it does not exist create it)
        // args[2] = path to initial centroid.txt
        // Reiterating till the convergence
        // out = path to output folder in hdfs
        IN = args[0];
        OUT = args[1];
        String input = IN;
        String output = OUT + System.nanoTime();
        String again_input = output;
        int iteration = 0;
        boolean isdone = false;
        while (isdone == false) {
            // job definition stuff
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "KMeans");
            job.setJarByClass(KMeans.class);
            // add the appropriate centroids to the cache
            if (iteration == 0) {
                Path initialCentroidPath = new Path(input + CENTROID_FILE_NAME);
                // Load the initial centroids from the file in the distributed cache
                DistributedCache.addCacheFile(initialCentroidPath.toUri(), job.getConfiguration());
            } else {
                Path newCentroidsPath = new Path(again_input + OUTPUT_FILE_NAME);
                // upload the new centers file to hdfs. Overwrite any existing copy.
                DistributedCache.addCacheFile(newCentroidsPath.toUri(), job.getConfiguration());
            }

            // Set the input and output paths
            FileInputFormat.setInputPaths(job, new Path(input + DATA_FILE_NAME));
            FileOutputFormat.setOutputPath(job, new Path(output));

            // Set the mapper and reducer classes
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            // Set the key and value classes for the mapper and reducer outputs
            job.setMapOutputKeyClass(PointWritable.class);
            job.setMapOutputValueClass(PointWritable.class);
            job.setOutputKeyClass(PointWritable.class);
            job.setOutputValueClass(NullWritable.class);

            // Set the number of reduce tasks
            job.setNumReduceTasks(1);

            // execute the job and wait for output
            job.waitForCompletion(true);
            // once the job is finished the final output is spit out to outputNanotime/
            // it consists of text: newCenterX newCenterY null
            // so we need to get this file and read the coordinates of the new centers
            Path ofile = new Path(output + OUTPUT_FILE_NAME);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
            List<PointWritable> centers_next = new ArrayList<PointWritable>();
            // for each line = newCenterX newCenterY
            String line = br.readLine();
            while (line != null) {
                String[] parts = line.split(" ");
                // get the first 2 elements which are the X , Y of the new center
                // and construct a PointWritable
                PointWritable center = new PointWritable(Double.parseDouble(parts[0]) , Double.parseDouble(parts[1]));
                centers_next.add(center);
                line = br.readLine();
            }
            br.close();

            // get the previous centers from the file they are located
            String prev;
            if (iteration == 0) {
                prev = input + CENTROID_FILE_NAME;
            } else {
                prev = again_input + OUTPUT_FILE_NAME;
            }
            Path prevfile = new Path(prev);
            FileSystem fs1 = FileSystem.get(new Configuration());
            BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(prevfile)));
            List<PointWritable> centers_prev = new ArrayList<PointWritable>();
            line = br1.readLine();
            while (line != null) {
                String[] parts = line.split(" ");
                PointWritable center = new PointWritable(Double.parseDouble(parts[0]) , Double.parseDouble(parts[1]));
                centers_prev.add(center);
                line = br1.readLine();
            }
            br1.close();

            // Sort the old centroid and new centroid and check for convergence condition
            //we sort them to check if they are close enough one by one
            // sorting is done based on the x coordinate
            Collections.sort(centers_next, new Comparator<PointWritable>() {
                public int compare(PointWritable p1, PointWritable p2) {
                    return Double.compare(p1.getX(), p2.getX());
                }
            });

            Collections.sort(centers_prev, new Comparator<PointWritable>() {
                public int compare(PointWritable p1, PointWritable p2) {
                    return Double.compare(p1.getX(), p2.getX());
                }
            });

            Iterator<PointWritable> it = centers_prev.iterator();

            for (PointWritable old_center : centers_next) {
                PointWritable new_center = it.next();
                // got the 2 centers
                // now check if they are close enough
                double dist = calculateDistanceBetweenPoints(old_center.getX() , old_center.getY() , new_center.getX() , new_center.getY());
                if (dist <= CONVERGENCE_THRESHOLD) {
                    // if they are , terminate
                    // the output should be in the latest ouputNanoTime folder
                    isdone = true;
                } else {
                    isdone = false;
                    break;
                }
            }
            ++iteration;
            again_input = output;
            output = args[1] + System.nanoTime();
        }
    }
}
