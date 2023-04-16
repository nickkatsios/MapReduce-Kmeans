package gr.aueb.dmst.nickkatsios;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class KmeansRunner {

    public static void main(String[] args) throws Exception {
        // Parse command-line arguments
        String inputPath = args[0];
        String outputPath = args[1];
        String centersPath = args[2];

        // Set up a Hadoop configuration and create a job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans");
        // Set the main class and jar file for the job
        job.setJarByClass(KmeansRunner.class);
        job.setMapperClass(KmeansMapper.class);
        job.setReducerClass(KmeansReducer.class);

        // Set the output key and value classes for the job
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Point2DWritable.class);

        // Set the input and output paths for the job
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Open the centers file using the FileSystem API
        FileSystem fs = FileSystem.get(conf);
        Path centersFilePath = new Path(centersPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centersFilePath)));

        // Read the centers from the file and store them in an ArrayList
        ArrayList<Point2D.Double> centers = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(" ");
            double x = Double.parseDouble(tokens[0]);
            double y = Double.parseDouble(tokens[1]);
            centers.add(new Point2D.Double(x, y));
        }
        br.close();

        // Convert the centers to Point2DWritable objects and store them in the job configuration
        Point2DWritable[] centersWritable = new Point2DWritable[centers.size()];
        for (int i = 0; i < centers.size(); i++) {
            centersWritable[i] = new Point2DWritable(centers.get(i));
        }
        conf.set("centers", Point2DWritable.arrayToString(centersWritable));

        // Set the number of clusters and convergence threshold in the job configuration
        conf.setInt("numClusters", centers.size());
        conf.setDouble("convergenceThreshold", 0.01);

        // Wait for the job to complete and print the results
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
