package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import saaf.Inspector;
import saaf.Response;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class LoadData implements RequestHandler<Request, HashMap<String, Object>> {

    public static HashMap<String, ArrayList<String>> parseCSV(InputStream objectData, ArrayList<String> headerList) {
        // Containing the headers
        ArrayList<String> headers = new ArrayList<>();

        // Containing the raw data
        HashMap<String, ArrayList<String>> rawData = new HashMap<>();

        Scanner scanner = new Scanner(objectData);

        // Read the headlines
        Scanner lineReader = new Scanner(scanner.nextLine());
        lineReader.useDelimiter(",");
        // Init the headers
        while (lineReader.hasNext()) {
            String header = lineReader.next();
            headers.add(header);
            ArrayList<String> tmp = new ArrayList<>();
            rawData.put(header, tmp);
        }

        // Read the content of the csv
        while (scanner.hasNext()) {
            String text = scanner.nextLine();
            // // Read the numbers
            lineReader = new Scanner(text);
            lineReader.useDelimiter(",");
            int headerIndex = 0;
            while (lineReader.hasNext()) {
                String data = lineReader.next();
                rawData.get(headers.get(headerIndex)).add(data);
                headerIndex++;
            }
            lineReader.close();

        }
        scanner.close();
        headerList = headers;
        return rawData;
    }
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    public static String PerformLoad(String bucketname, String filename, AmazonS3 s3Client) {
       
        // get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        // get content of the file
        InputStream objectData = s3Object.getObjectContent();
        ArrayList<String> headers = new ArrayList<>();
        HashMap<String, ArrayList<String>> processedData = new HashMap<String, ArrayList<String>>();
        processedData = parseCSV(objectData, headers);
         Properties properties = new Properties();
         try {
            properties.load(new FileInputStream("db.properties"));
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            Connection con = DriverManager.getConnection(url, username, password);

            //All ArrayList has the same size
            int dataSize = processedData.get("Region").size();
    
            String insertQuery = "INSERT INTO SalesData (Region, Country, ItemType, SalesChannel, OrderPriority, OrderDate, OrderID, ShipDate, "
        + "UnitsSold, UnitPrice, UnitCost, TotalRevenue, TotalCost, TotalProfit,GrossMargin,OrderProcessingTime) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; 
            try (PreparedStatement ps = con.prepareStatement(insertQuery)) {
                  int batchSize = 100;
                  for (int i=0 ; i<dataSize ; i++) {
                     ps.setString(1, processedData.get("Region").get(i));
                     ps.setString(2, processedData.get("Country").get(i));
                     ps.setString(3, processedData.get("Item Type").get(i));
                     ps.setString(4, processedData.get("Sales Channel").get(i));
                     ps.setString(5, processedData.get("Order Priority").get(i));
                     ps.setString(6, processedData.get("Order Date").get(i));
                     ps.setInt(7, Integer.valueOf(processedData.get("Order ID").get(i)));
                     ps.setString(8, processedData.get("Ship Date").get(i));
                     ps.setInt(9, Integer.valueOf(processedData.get("Units Sold").get(i)));
                     ps.setDouble(10, Double.valueOf(processedData.get("Unit Price").get(i)));
                     ps.setDouble(11, Double.valueOf(processedData.get("Unit Cost").get(i)));
                     ps.setDouble(12, Double.valueOf(processedData.get("Total Revenue").get(i)));
                     ps.setDouble(13, Double.valueOf(processedData.get("Total Cost").get(i)));
                     ps.setDouble(14, Double.valueOf(processedData.get("Total Profit").get(i)));
                     ps.setFloat(15, Float.valueOf(processedData.get("Gross Margin").get(i)));
                     ps.setInt(16, Integer.valueOf(processedData.get("Order Processing Time").get(i)));
                     ps.addBatch();
                     if ((i+1)%batchSize==0 || i == dataSize-1 ) {
                        ps.executeBatch();
                        ps.clearBatch();
                     }
                  }               
                     
            }
            con.close();
        } catch (Exception e) {
            System.out.println("Got an exception working with MySQL! ");
            System.out.println(e.getMessage());
        }
        String finalRes= "Database: " + properties.getProperty("database") + " Table:" + properties.getProperty("table") + " processed.";
        return finalRes;
    }
    /**
     * Lambda Function Handler
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        // ****************START FUNCTION IMPLEMENTATION*************************
        // Add custom key/value attribute to SAAF's output. (OPTIONAL)

        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        
        //Perform transform
        String result = PerformLoad(bucketname,  filename, s3Client);


        // (OPTIONAL)
        LambdaLogger logger = context.getLogger();
        logger.log("test: "+result);
        // logger.log("ProcessCSV bucketname:" + bucketname + " filename:" + filename +
        // " avg-element:" + avg + " total:"
        // + total);
        Response response = new Response();

        // Set response value
        response.setValue(result);

        inspector.consumeResponse(response);

        // ****************END FUNCTION IMPLEMENTATION***************************

        // Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}