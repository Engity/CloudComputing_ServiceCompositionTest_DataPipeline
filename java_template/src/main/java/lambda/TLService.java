package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;
import saaf.Response;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class TLService implements RequestHandler<Request, HashMap<String, Object>>  {

    public static String PerformTL(String bucketname, String filename, AmazonS3 s3Client) {

        // Transform

        // get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));

        // get content of the file
        InputStream objectData = s3Object.getObjectContent();
        ArrayList<String> headerList = new ArrayList<>();

        ArrayList<ArrayList<String>> rawData = new ArrayList<>();

        Scanner scanner = new Scanner(objectData);
        StringBuilder sw = new StringBuilder();
        // Read the headlines
        Scanner lineReader = new Scanner(scanner.nextLine());
        lineReader.useDelimiter(",");
        // Init the headers
        while (lineReader.hasNext()) {
            String header = lineReader.next();
            headerList.add(header);
        }

        headerList.add("Gross Margin");
        headerList.add("Order Processing Time");
       
        rawData.add(headerList);
        HashSet<String> orderIDSets = new HashSet<>();

        SimpleDateFormat myFormat = new SimpleDateFormat("MM/dd/yyyy");
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
                if (headerIndex == 4) {// Order priority
                    switch (data) {
                        case "L":
                            data = "Low";
                            break;
                        case "M":
                            data = "Medium";
                            break;
                        case "H":
                            data = "High";
                            break;
                        case "C":
                            data = "Critical";
                            break;
                    }
                }

                tmp.add(data);
                headerIndex++;
            }
            // Only process orderID that does not duplicate
            String orderId = tmp.get(6);
            if (!orderIDSets.contains(orderId)) {
                Double profit = Double.parseDouble(tmp.get(13));
                Double revenue = Double.parseDouble(tmp.get(11));
                Double grossMargin = profit / revenue;
                tmp.add(grossMargin.toString());

                String inputString1 = tmp.get(5);
                String inputString2 = tmp.get(7);

                try {
                    Date date1 = myFormat.parse(inputString1);
                    Date date2 = myFormat.parse(inputString2);
                    Long diff = (date2.getTime() - date1.getTime()) / 86400000;
                    tmp.add(diff.toString());
                } catch (Exception e) {
                    tmp.add("INVALID DATE");
                }
               
                rawData.add(tmp);
            }

            lineReader.close();

        }
        scanner.close();

        //Load:
        Properties properties = new Properties();
         try {
           
           
            properties.load(new FileInputStream("db.properties"));

            
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            Connection con = DriverManager.getConnection(url, username, password);

            //All ArrayList has the same size
            int dataSize = rawData.size();
    
            String insertQuery = "INSERT INTO SalesData (Region, Country, ItemType, SalesChannel, OrderPriority, OrderDate, OrderID, ShipDate, "
        + "UnitsSold, UnitPrice, UnitCost, TotalRevenue, TotalCost, TotalProfit,GrossMargin,OrderProcessingTime) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; 
            try (PreparedStatement ps = con.prepareStatement(insertQuery)) {
                  int batchSize = 100;
                  for (int i=1 ; i<dataSize ; i++) {
                     ps.setString(1, rawData.get(i).get(0));
                     ps.setString(2, rawData.get(i).get(1));
                     ps.setString(3, rawData.get(i).get(2));
                     ps.setString(4, rawData.get(i).get(3));
                     ps.setString(5, rawData.get(i).get(4));
                     ps.setString(6, rawData.get(i).get(5));
                     ps.setInt(7, Integer.valueOf(rawData.get(i).get(6)));
                     ps.setString(8, rawData.get(i).get(7));
                     ps.setInt(9, Integer.valueOf(rawData.get(i).get(8)));
                     ps.setDouble(10, Double.valueOf(rawData.get(i).get(9)));
                     ps.setDouble(11, Double.valueOf(rawData.get(i).get(10)));
                     ps.setDouble(12, Double.valueOf(rawData.get(i).get(11)));
                     ps.setDouble(13, Double.valueOf(rawData.get(i).get(12)));
                     ps.setDouble(14, Double.valueOf(rawData.get(i).get(13)));
                     ps.setFloat(15, Float.valueOf(rawData.get(i).get(14)));
                     ps.setInt(16, Integer.valueOf(rawData.get(i).get(15)));
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
       

        String finalRes = "Filename:" + filename + " processed with " + headerList.size() + " columns and "
                + rawData.get(0).size() + " rows.\n And loaded into Database"+properties.getProperty("database")+" and table: "+properties.getProperty("table");
        return finalRes;
    }

    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        // ****************START FUNCTION IMPLEMENTATION*************************
        // Add custom key/value attribute to SAAF's output. (OPTIONAL)

        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        
        //Perform transform
        String result = PerformTL(bucketname,  filename, s3Client);




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
