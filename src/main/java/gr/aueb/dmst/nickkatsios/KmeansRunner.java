package gr.aueb.dmst.nickkatsios;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KmeansRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: KMeansMapReduce <inputPath> <outputPath> <centersPath>");
            System.exit(-1);
        }
        args[0] = "hdfs://localhost:9000/user/user/ex_data/data.txt";
        args[1] = "./output";
        args[2] = "hdfs://localhost:9000/user/user/ex_data/centers.txt";
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path centersPath = new Path(args[2]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "K-Means MapReduce");

        // Load the initial centers from a file and put them into the distributed cache
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new FileReader(centersPath.toString()));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(" ");
            if (parts.length == 2) {
                Point2D.Double center = new Point2D.Double(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
                job.addCacheFile(new Path(centersPath, center.toString()).toUri());
            }
        }
        reader.close();

        job.setJarByClass(KmeansRunner.class);

        job.setMapperClass(KmeansMapper.class);
        job.setReducerClass(KmeansReducer.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
