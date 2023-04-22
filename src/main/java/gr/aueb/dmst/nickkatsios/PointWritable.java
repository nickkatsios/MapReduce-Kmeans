package gr.aueb.dmst.nickkatsios;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

// Custom class for handling (x,y) data in hadoop
// Just like a Point2D.Double
public class PointWritable implements WritableComparable<PointWritable> {
    private DoubleWritable x;
    private DoubleWritable y;

    public PointWritable() {
        this.x = new DoubleWritable();
        this.y = new DoubleWritable();
    }

    public PointWritable(double x, double y) {
        this.x = new DoubleWritable(x);
        this.y = new DoubleWritable(y);
    }

    public double getX() {
        return x.get();
    }

    public double getY() {
        return y.get();
    }

    // writing to context
    @Override
    public void write(DataOutput out) throws IOException {
        x.write(out);
        y.write(out);
    }

    // reading from context
    @Override
    public void readFields(DataInput in) throws IOException {
        x.readFields(in);
        y.readFields(in);
    }

    // comparing keys for reducing
    @Override
    public int compareTo(PointWritable other) {
        int xCompare = x.compareTo(other.x);
        if (xCompare == 0) {
            return y.compareTo(other.y);
        }
        return xCompare;
    }

    // unique hash for each point
    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }

}