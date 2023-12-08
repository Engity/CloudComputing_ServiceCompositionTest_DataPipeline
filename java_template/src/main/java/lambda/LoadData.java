package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.google.gson.JsonArray;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

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

    /**
     * Lambda Function Handler
     *
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        // Process data in CSV
        String bucketname = request.getBucketname();
        String filename = "Transform_result.csv";

        StringWriter sw = new StringWriter();
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");
        meta.setContentLength(bytes.length);
        // Create new file on S3
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        // get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));

        // get content of the file
        InputStream objectData = s3Object.getObjectContent();
        ArrayList<String> headers = new ArrayList<>();
        HashMap<String, ArrayList<String>> processedData = new HashMap<String, ArrayList<String>>();
        processedData = parseCSV(objectData, headers);

        LambdaLogger logger = context.getLogger();
        logger.log("ProcessCSV bucketname:" + bucketname + " filename:" + filename);

        Response response = new Response();
        // Load data into database.
        Properties properties = new Properties();
        try {

            properties.load(new FileInputStream("db.properties"));

            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            Connection con = DriverManager.getConnection(url, username, password);

            // All ArrayList has the same size
            int dataSize = processedData.get("Region").size();

            String insertQuery = "INSERT INTO SalesData (Region, Country, ItemType, SalesChannel, OrderPriority, OrderDate, OrderID, ShipDate, "
                    + "UnitsSold, UnitPrice, UnitCost, TotalRevenue, TotalCost, TotalProfit,GrossMargin,OrderProcessingTime) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = con.prepareStatement(insertQuery)) {
                int batchSize = 100;
                for (int i = 0; i < dataSize; i++) {
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
                    if ((i + 1) % batchSize == 0 || i == dataSize - 1) {
                        ps.executeBatch();
                        ps.clearBatch();
                    }
                }
            }
            // Execute Query 1: All data from database
            PreparedStatement ps = con.prepareStatement(
                    "SELECT * FROM SalesData;");
            ResultSet rs = ps.executeQuery();
            String formattedJson_1 = resultSetToJson(rs);
            rs.close();

            // Execute Query 2: Total cost and Total Revenue for each country
            ps = con.prepareStatement(
                    "SELECT Country, SUM(TotalRevenue) AS Total Revenue, SUM(TotalCost) AS Total Cost FROM SalesData GROUP BY Country;");
            ps.execute();
            rs = ps.executeQuery();
            String formattedJson_2 = resultSetToJson(rs);
            rs.close();

            // Execute Query 3: Countries with profit revenue above 30%
            ps = con.prepareStatement(
                    "SELECT Country, AVG((TotalProfit / TotalRevenue) * 100) AS Profit Margin FROM SalesData GROUP BY Country HAVING Profit Margin > 30");
            ps.execute();
            rs = ps.executeQuery();
            String formattedJson_3 = resultSetToJson(rs);
            rs.close();

            // Execute Query 4: Top 15 items sold
            ps = con.prepareStatement(
                    "SELECT ItemType, SUM(UnitsSold) AS Total Units Sold FROM SalesData GROUP BY ItemType ORDER BY Total Units Sold DESC LIMIT 15");
            ps.execute();
            rs = ps.executeQuery();
            String formattedJson_4 = resultSetToJson(rs);
            rs.close();

            // Execute Query 5: Total revenue each year for each region
            ps = con.prepareStatement(
                    "SELECT YEAR(STR_TO_DATE(OrderDate, '%m/%d/%Y')) AS Sales Year, Region, SUM(TotalRevenue) AS Total Revenue FROM SalesData GROUP BY Region, Sales Year ORDER BY Region, Sales Year ASC, Total Revenue DESC");
            rs = ps.executeQuery();
            String formattedJson_5 = resultSetToJson(rs);
            rs.close();

            con.close();
            // Merging the JSON strings
            JsonObject mergedJson = new JsonObject();
            mergedJson.add("Query 1", new Gson().fromJson(formattedJson_1, JsonObject.class));
            mergedJson.add("Query 2", new Gson().fromJson(formattedJson_2, JsonObject.class));
            mergedJson.add("Query 3", new Gson().fromJson(formattedJson_3, JsonObject.class));
            mergedJson.add("Query 4", new Gson().fromJson(formattedJson_4, JsonObject.class));
            mergedJson.add("Query 5", new Gson().fromJson(formattedJson_5, JsonObject.class));
            response.setJsonResult(mergedJson.toString());

            con.close();
        } catch (Exception e) {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
        }

        response.setValue("Database: " + properties.getProperty("database") + " Table:"
                + properties.getProperty("table") + " processed.");

        inspector.consumeResponse(response);

        // ****************END FUNCTION IMPLEMENTATION***************************
        // Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private String resultSetToJson(ResultSet rs) throws Exception {
        JsonObject jsonResult = new JsonObject();
        JsonArray columns = new JsonArray();
        JsonArray rows = new JsonArray();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            columns.add(metaData.getColumnName(i));
        }

        while (rs.next()) {
            JsonObject row = new JsonObject();
            for (int i = 1; i <= columnCount; i++) {
                row.addProperty(metaData.getColumnName(i), rs.getString(i));
            }
            rows.add(row);
        }

        jsonResult.add("columns", columns);
        jsonResult.add("rows", rows);
        return jsonResult.toString();
    }
}