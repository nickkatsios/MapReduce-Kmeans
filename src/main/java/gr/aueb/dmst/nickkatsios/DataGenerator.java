package gr.aueb.dmst.nickkatsios;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataGenerator {
    public static void main(String[] args) throws IOException {
        int numPoints = 1100000; // Number of data points to generate

        // Define the centers of the three clusters
        double x1 = -50000.0;
        double y1 = -50000.0;
        double x2 = 50000.0;
        double y2 = 50000.0;
        double x3 = 0.0;
        double y3 = 0.0;

        // Define the skewness of the distribution from which distances are drawn
        double skewness = 2.0;

        // Open a file writer to write the data points to a text file
        FileWriter writer;
        try {
            writer = new FileWriter("data.txt");
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // Generate the data points and write them to the text file
        Random rand = new Random();
        for (int i = 0; i < numPoints; i++) {
            double x, y;
            double r = rand.nextDouble();
            if (i % 3 == 0) {
                r = Math.pow(r, skewness);
                x = x1 + r * (rand.nextDouble() * 200000.0 - 100000.0);
                y = y1 + r * (rand.nextDouble() * 200000.0 - 100000.0);
            } else if (i % 3 == 1) {
                r = Math.pow(r, skewness);
                x = x2 + r * (rand.nextDouble() * 200000.0 - 100000.0);
                y = y2 + r * (rand.nextDouble() * 200000.0 - 100000.0);
            } else {
                r = Math.pow(r, skewness);
                x = x3 + r * (rand.nextDouble() * 200000.0 - 100000.0);
                y = y3 + r * (rand.nextDouble() * 200000.0 - 100000.0);
            }
            writer.write(String.format("%.3f %.3f\n", x, y));
        }

        // Close the file writer
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
