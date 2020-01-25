package print_result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;

import utils.utils;
public class PrintResult extends ApplicationFrame{

    public PrintResult(String applicationTitle , String chartTitle) throws IOException {
        super( applicationTitle );
        JFreeChart barChart = ChartFactory.createBarChart(
                chartTitle,
                "Sentiment class",
                "Number of tweets",
                getCurrentScore(),
                PlotOrientation.VERTICAL,
                true, true, false);

        ChartPanel chartPanel = new ChartPanel(barChart);
        chartPanel.setPreferredSize(new java.awt.Dimension( 500 , 500 ) );
        setContentPane(chartPanel);
    }
    private CategoryDataset getCurrentScore() throws IOException {
        DefaultCategoryDataset scores = new DefaultCategoryDataset( );
        if (hTable!=null) {
            for (String category : classifications) {
                Get g = new Get(Bytes.toBytes(category));
                Result resultTable = hTable.get(g);
                byte[] values = resultTable.getValue(Bytes.toBytes("sentiment"), Bytes.toBytes("value"));
                int sentimentOfClass = 0;
                if (Bytes.toString(values) != null && !utils.allZeros(values)) {
                    sentimentOfClass = Integer.parseInt(Bytes.toString(values));
                }
                scores.addValue(sentimentOfClass, category, "");
            }
        }

        return scores;
    }
    private static Table hTable;
    private static String[] classifications={"Negative","Neutral","Positive"};
    public static void main(String[] args) throws IOException, InterruptedException {
        String QUERY="";
        if (args.length > 0)
             QUERY= args[0];
        if(QUERY.isEmpty())
            System.exit(1);


        PrintResult plot= new PrintResult("Twitter sentiment analyzer ", "Query: " + QUERY);
        plot.pack();
        RefineryUtilities.centerFrameOnScreen(plot);
        plot.setVisible(true);

        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin  = connection.getAdmin();

        if (admin.tableExists(TableName.valueOf(QUERY))) {
            hTable = connection.getTable(TableName.valueOf(QUERY));
            while(true){
                JFreeChart barChart = ChartFactory.createBarChart(
                        "Query: " + QUERY,
                        "Sentiment class",
                        "Number of tweets",
                        plot.getCurrentScore(),
                        PlotOrientation.VERTICAL,
                        true, true, false);
                ChartPanel chartPanel = new ChartPanel(barChart);
                chartPanel.setPreferredSize(new java.awt.Dimension( 400 , 350 ) );
                plot.setContentPane(chartPanel);

                plot.pack();
                Thread.sleep(5*1000);
            }
        }
        else{
            System.out.println("ERROR: query not exist in hbase");
            System.exit(1);
        }
    }
}
