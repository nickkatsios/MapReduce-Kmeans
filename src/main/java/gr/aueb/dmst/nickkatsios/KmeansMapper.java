package gr.aueb.dmst.nickkatsios;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class KmeansMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private final List<Point2D.Double> centers = new ArrayList<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load the initial centers from a file
        // For simplicity, we assume that the initial centers are provided as a file in the distributed cache
        // You can also use a database or other external storage to store the centers
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
                    centers.add(center);
                }
            }
            reader.close();
        }
    }

    /**
     *
     * @param x1 the x of the first point
     * @param y1 the y of the first point
     * @param x2 the x of the second point
     * @param y2 the y of the second point
     * @return the euclidean distance between the 2 points
     */
    public double calculateDistanceBetweenPoints(
            double x1,
            double y1,
            double x2,
            double y2) {
        return Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the point from the input value
        String[] parts = value.toString().split(" ");
        if (parts.length == 2) {
            Point2D.Double point = new Point2D.Double(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
            // Find the nearest center for the point
            int nearestCenter = -1;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centers.size(); i++) {
                Point2D.Double center = centers.get(i);
                double distance = calculateDistanceBetweenPoints(point.getX() , point.getY() , center.getX() , center.getY());
                if (distance < minDistance) {
                    nearestCenter = i;
                    minDistance = distance;
                }
            }

            // Emit the point with the nearest center as the key
            context.write(new DoubleWritable(nearestCenter), value);
        }
    }
}
