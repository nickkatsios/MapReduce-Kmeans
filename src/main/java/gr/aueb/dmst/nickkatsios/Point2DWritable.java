package gr.aueb.dmst.nickkatsios;

import org.apache.hadoop.io.Writable;

import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point2DWritable implements Writable {
    private double x;
    private double y;

    public Point2DWritable() {}

    public Point2DWritable(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point2DWritable(Point2D.Double point) {
        this.x = point.getX();
        this.y = point.getY();
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readDouble();
        y = in.readDouble();
    }

    public static Point2DWritable[] stringToArray(String s) {
        String[] tokens = s.split(";");
        Point2DWritable[] array = new Point2DWritable[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            String[] coords = tokens[i].split(",");
            double x = Double.parseDouble(coords[0]);
            double y = Double.parseDouble(coords[1]);
            array[i] = new Point2DWritable(x, y);
        }
        return array;
    }

    public static String arrayToString(Point2DWritable[] array) {
        StringBuilder sb = new StringBuilder();
        for (Point2DWritable point : array) {
            sb.append(point.getX());
            sb.append(",");
            sb.append(point.getY());
            sb.append(";");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "(" + x + ", " + y + ")";
    }
}

