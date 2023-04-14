package gr.aueb.dmst.nickkatsios;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class DataGenerator {
    public static void main(String[] args) throws IOException {
        // Define the number of data points
        int numPoints = 1500000;

        // Define the centers of the three clusters
        double x1 = 0.5;
        double y1 = 0.5;
        double x2 = -0.5;
        double y2 = -0.5;
        double x3 = 1.0;
        double y3 = -1.0;

        // Define the standard deviation for the random distance
        double stdDev = 0.1;

        // Generate the data points and write them to a file
        Random rand = new Random();
        File file = new File("data.txt");
        FileWriter writer = new FileWriter(file);
        for (int i = 0; i < numPoints; i++) {
            double randX, randY;
            double randDist = Math.abs(rand.nextGaussian()) * stdDev;
            int cluster = rand.nextInt(3);
            switch (cluster) {
                case 0:
                    randX = x1 + randDist * rand.nextGaussian();
                    randY = y1 + randDist * rand.nextGaussian();
                    break;
                case 1:
                    randX = x2 + randDist * rand.nextGaussian();
                    randY = y2 + randDist * rand.nextGaussian();
                    break;
                default:
                    randX = x3 + randDist * rand.nextGaussian();
                    randY = y3 + randDist * rand.nextGaussian();
                    break;
            }
            writer.write(randX + " " + randY + "\n");
        }
        writer.close();
    }
}