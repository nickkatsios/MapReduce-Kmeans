package gr.aueb.dmst.nickkatsios;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.swing.JFrame;
import javax.swing.JPanel;

public class DataPlotter extends JPanel {
    private List<Double> xValues;
    private List<Double> yValues;

    public DataPlotter() {
        super();
        xValues = new ArrayList<Double>();
        yValues = new ArrayList<Double>();

        // Read the data points from the file
        try (BufferedReader br = new BufferedReader(new FileReader("data.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(" ");
                xValues.add(Double.parseDouble(values[0]));
                yValues.add(Double.parseDouble(values[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g);
        Graphics2D g2d = (Graphics2D) g;

        // Set anti-aliasing for smoother graphics
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        // Set background color
        g2d.setBackground(Color.WHITE);

        // Set point color and size
        g2d.setColor(Color.BLUE);
        int pointSize = 3;

        // Scale factor to fit the data points on the panel
        double scaleX = getWidth() / 200000.0;
        double scaleY = getHeight() / 200000.0;

        // Plot the data points
        for (int i = 0; i < xValues.size(); i++) {
            int x = (int) (xValues.get(i) * scaleX + getWidth() / 2.0);
            int y = (int) (yValues.get(i) * scaleY + getHeight() / 2.0);
            g2d.fillOval(x, y, pointSize, pointSize);
        }
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("Data Plotter");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(800, 800);
        DataPlotter plotter = new DataPlotter();
        frame.add(plotter);
        frame.setVisible(true);
    }
}
