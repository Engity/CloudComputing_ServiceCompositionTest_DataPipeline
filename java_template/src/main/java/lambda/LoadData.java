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

    public static ArrayList<ArrayList<String>> parseCSV(InputStream objectData, ArrayList<String> headerList) {
        // Containing the raw data
        ArrayList<ArrayList<String>> rawData = new ArrayList<>();

        Scanner scanner = new Scanner(objectData);

        // Read the headlines
        Scanner lineReader = new Scanner(scanner.nextLine());
        lineReader.useDelimiter(",");
        // Init the headers
        while (lineReader.hasNext()) {
            String header = lineReader.next();
            headerList.add(header);
        }
        rawData.add(headerList);

        // Read the content of the csv
        while (scanner.hasNext()) {
            String text = scanner.nextLine();
            // // Read the numbers
            lineReader = new Scanner(text);
            lineReader.useDelimiter(",");
            int headerIndex = 0;
            ArrayList<String> tmp = new ArrayList<>();
            while (lineReader.hasNext()) {
                String data = lineReader.next();
                tmp.add(data);
                headerIndex++;
            }

            rawData.add(tmp);
            lineReader.close();

        }
        scanner.close();
        return rawData;
    }

  

    public static String PerformLoad(String bucketname, String filename, AmazonS3 s3Client) {
       
        // get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        // get content of the file
        InputStream objectData = s3Object.getObjectContent();
        ArrayList<String> headers = new ArrayList<>();
        ArrayList<ArrayList<String>> processedData = new ArrayList<ArrayList<String>>();
        processedData = parseCSV(objectData, headers);
         Properties properties = new Properties();
         try {
            properties.load(new FileInputStream("db.properties"));
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            Connection con = DriverManager.getConnection(url, username, password);

            //All ArrayList has the same size
            int dataSize = processedData.size();
            System.out.println("datasize: "+dataSize);
            String insertQuery = "INSERT INTO SalesData (ID,Region, Country, ItemType, SalesChannel, OrderPriority, OrderDate, OrderID, ShipDate, "
        + "UnitsSold, UnitPrice, UnitCost, TotalRevenue, TotalCost, TotalProfit,GrossMargin,OrderProcessingTime) "
        + "VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; 
        try (PreparedStatement ps = con.prepareStatement(insertQuery)) {
            int batchSize = 100;
            for (int i=1 ; i<dataSize ; i++) {
                ps.setInt(1, i);
                ps.setString(2, processedData.get(i).get(0));
                ps.setString(3, processedData.get(i).get(1));
                ps.setString(4, processedData.get(i).get(2));
                ps.setString(5, processedData.get(i).get(3));
                ps.setString(6, processedData.get(i).get(4));
                ps.setString(7, processedData.get(i).get(5));
                ps.setInt(8, Integer.valueOf(processedData.get(i).get(6)));
                ps.setString(9, processedData.get(i).get(7));
                ps.setInt(10, Integer.valueOf(processedData.get(i).get(8)));
                ps.setDouble(11, Double.valueOf(processedData.get(i).get(9)));
                ps.setDouble(12, Double.valueOf(processedData.get(i).get(10)));
                ps.setDouble(13, Double.valueOf(processedData.get(i).get(11)));
                ps.setDouble(14, Double.valueOf(processedData.get(i).get(12)));
                ps.setDouble(15, Double.valueOf(processedData.get(i).get(13)));
                ps.setFloat(16, Float.valueOf(processedData.get(i).get(14)));
                ps.setInt(17, Integer.valueOf(processedData.get(i).get(15)));
                ps.addBatch();
               if (i%batchSize==0 || i == dataSize-1 ) {
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
        String filename = "Transform_result.csv";
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
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