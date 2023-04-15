package gr.aueb.dmst.nickkatsios;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KmeansReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    private final Map<Double, Point2D.Double> oldCenters = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load the old centers from the distributed cache
        Path[] files = context.getLocalCacheFiles();
        if (files != null && files.length > 0) {
            // Read the centers from the file
            // Each line in the file contains a center with x and y coordinates separated by a comma
            // For example: 1.0,2.0
            BufferedReader reader = new BufferedReader(new FileReader(files[0].toString()));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    Point2D.Double center = new Point2D.Double(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
                    oldCenters.put(Double.parseDouble(parts[2]), center);
                }
            }
            reader.close();
        }
    }


    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Compute the new center for the cluster
        double sumX = 0.0;
        double sumY = 0.0;
        int count = 0;
        for (Text value : values) {
            String[] parts = value.toString().split(" ");
            if (parts.length == 2) {
                Point2D.Double point = new Point2D.Double(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
                sumX += point.getX();
                sumY += point.getY();
                count++;
            }
        }
        Point2D.Double newCenter = new Point2D.Double(sumX / count, sumY / count);

        // Check if the difference between the old and new center is less than 0.01
        Point2D.Double oldCenter = oldCenters.get(key.get());
        if (oldCenter != null && Math.abs(oldCenter.getX() - newCenter.getX()) < 0.01
                && Math.abs(oldCenter.getY() - newCenter.getY()) < 0.01) {
            // If the difference is less than 0.01, stop the algorithm by writing the final centers to the output
            for (Map.Entry<Double, Point2D.Double> entry : oldCenters.entrySet()) {
                context.write(new Text(String.valueOf(entry.getKey())), new Text(entry.getValue().getX() + "," + entry.getValue().getY()));
            }
            return;
        }

        // Otherwise, emit the new center as a string in the format "x,y" with the cluster ID as the key
        context.write(new Text(String.valueOf(key.get())), new Text(newCenter.getX() + "," + newCenter.getY()));
    }
}
