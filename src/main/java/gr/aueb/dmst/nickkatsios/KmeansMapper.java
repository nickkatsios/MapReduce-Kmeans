package gr.aueb.dmst.nickkatsios;

import java.awt.*;
import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
public class KmeansMapper extends MapReduceBase implements Mapper<Point2D.Double,Point2D.Double, Point2D.Double,Point2D.Double>{

    // Define the centers of the three clusters
        double x1 = 0.5;
        double y1 = 0.5;
        Point2D.Double center1 = new Point2D.Double(x1 ,y1);
        double x2 = -0.5;
        double y2 = -0.5;
        Point2D.Double center2 = new Point2D.Double(x2 ,y2);
        double x3 = 1.0;
        double y3 = -1.0;
        Point2D.Double center3 = new Point2D.Double(x3 ,y3);
        Point2D.Double[] centers = {center1 , center2 , center3};

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

    public void map(Point2D.Double key, Point2D.Double value, OutputCollector<Point2D.Double,Point2D.Double> output,
                    Reporter reporter) throws IOException{
        BufferedReader reader = new BufferedReader(new FileReader("data.txt"));
        String line = null;
        // iterate through the lines of the txt file
        while ((line = reader.readLine()) != null) {
            // split them at " "
            String[] points = line.split(" ");
            // extract the x , y coordinates
            double x = Double.parseDouble(points[0]);
            double y = Double.parseDouble(points[1]);
            // construct the point
            Point2D.Double point = new Point2D.Double(x ,y);
            // calculate the distance fom each center and keep the minimum
            Point2D.Double nearest_center = centers[0];
            double minDist = Double.MAX_VALUE;
            for(int i = 0 ; i < centers.length ; i++) {
                double dist = calculateDistanceBetweenPoints(point.getX() , point.getY() , centers[i].getX() , centers[i].getY());
                if(dist < minDist) {
                    minDist = dist;
                    nearest_center = centers[i];
                }
            }
            // Emit the nearest center and the point to the reducer
            output.collect(nearest_center, point);

        }

    }

}