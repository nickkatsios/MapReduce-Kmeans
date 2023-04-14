package gr.aueb.dmst.nickkatsios;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import javax.swing.JFrame;
import javax.swing.JPanel;

public class DataPlotter {
    public static void main(String[] args) throws IOException {
        // Read the data points from the file
        BufferedReader reader = new BufferedReader(new FileReader("data.txt"));
        double[][] points = new double[1500000][2];
        String line;
        int index = 0;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(" ");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);
            points[index][0] = x;
            points[index][1] = y;
            index++;
        }
        reader.close();

        // Create a panel to draw the points
        JPanel panel = new JPanel() {
            @Override
            protected void paintComponent(Graphics g) {
                super.paintComponent(g);
                Graphics2D g2d = (Graphics2D) g;
                g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                g2d.setColor(Color.BLUE);
                for (int i = 0; i < points.length; i++) {
                    double x = points[i][0];
                    double y = points[i][1];
                    int px = (int) (x * 100) + 400; // Scale the x coordinate and add an offset
                    int py = (int) (y * 100) + 400; // Scale the y coordinate and add an offset
                    g2d.drawOval(px - 2, py - 2, 4, 4); // Draw a small circle for each point
                }
            }
        };

        // Display the panel in a frame
        JFrame frame = new JFrame("Data Plot");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800, 800);
        frame.add(panel);
        frame.setVisible(true);
    }
}