package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import saaf.Inspector;
import saaf.Response;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;


/**
 *
 * @author kokinh11
 */
public class LQService implements RequestHandler<Request, HashMap<String, Object>> {

    public static String performLQ(Boolean saveCSV, String bucketname, String filename, ArrayList<String> headerList, AmazonS3 s3Client) {
        
        /////////Load////////
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        InputStream objectData = s3Object.getObjectContent();
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
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("db.properties"));
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            Connection con = DriverManager.getConnection(url, username, password);

            //All ArrayList has the same size
            int dataSize = rawData.get("Region").size();

            String insertQuery = "INSERT INTO SalesData (Region, Country, ItemType, SalesChannel, OrderPriority, OrderDate, OrderID, ShipDate, "
                    + "UnitsSold, UnitPrice, UnitCost, TotalRevenue, TotalCost, TotalProfit,GrossMargin,OrderProcessingTime) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = con.prepareStatement(insertQuery)) {
                int batchSize = 100;
                for (int i = 0; i < dataSize; i++) {
                    ps.setString(1, rawData.get("Region").get(i));
                    ps.setString(2, rawData.get("Country").get(i));
                    ps.setString(3, rawData.get("Item Type").get(i));
                    ps.setString(4, rawData.get("Sales Channel").get(i));
                    ps.setString(5, rawData.get("Order Priority").get(i));
                    ps.setString(6, rawData.get("Order Date").get(i));
                    ps.setInt(7, Integer.valueOf(rawData.get("Order ID").get(i)));
                    ps.setString(8, rawData.get("Ship Date").get(i));
                    ps.setInt(9, Integer.valueOf(rawData.get("Units Sold").get(i)));
                    ps.setDouble(10, Double.valueOf(rawData.get("Unit Price").get(i)));
                    ps.setDouble(11, Double.valueOf(rawData.get("Unit Cost").get(i)));
                    ps.setDouble(12, Double.valueOf(rawData.get("Total Revenue").get(i)));
                    ps.setDouble(13, Double.valueOf(rawData.get("Total Cost").get(i)));
                    ps.setDouble(14, Double.valueOf(rawData.get("Total Profit").get(i)));
                    ps.setFloat(15, Float.valueOf(rawData.get("Gross Margin").get(i)));
                    ps.setInt(16, Integer.valueOf(rawData.get("Order Processing Time").get(i)));
                    ps.addBatch();
                    if ((i + 1) % batchSize == 0 || i == dataSize - 1) {
                        ps.executeBatch();
                        ps.clearBatch();
                    }
                }
            }
            
            /////////Query////////
            String queryResults ="";
            String rawQueryResult ="";
            
            // Execute Query 1: All data from database
            PreparedStatement ps = con.prepareStatement(
                    "SELECT * FROM SalesData;");
            ResultSet rs = ps.executeQuery();
            if (saveCSV){
                rawQueryResult += resultSetToJson(rs, 1) + "\n";
                queryResults += resultSetToString(rs, 1) + "\n";
            } else {
                queryResults += resultSetToString(rs, 1) + "\n";
            }
            rs.close();

            // Execute Query 2: Total cost and Total Revenue for each country
            ps = con.prepareStatement(
                    "SELECT Country, SUM(TotalRevenue) AS Total_Revenue, SUM(TotalCost) AS Total_Cost FROM SalesData GROUP BY Country;");
            ps.execute();
            rs = ps.executeQuery();
            if (saveCSV){
                rawQueryResult += resultSetToJson(rs, 2) + "\n";
                queryResults += resultSetToString(rs, 2) + "\n";
            } else {
                queryResults += resultSetToString(rs, 2) + "\n";
            }
            rs.close();

            // Execute Query 3: Countries with profit revenue above 30%
            ps = con.prepareStatement(
                    "SELECT Country, AVG((TotalProfit / TotalRevenue) * 100) AS Profit_Margin FROM SalesData GROUP BY Country HAVING Profit_Margin > 30;");
            ps.execute();
            rs = ps.executeQuery();
            if (saveCSV) {
                rawQueryResult += resultSetToJson(rs, 3) + "\n";
                queryResults += resultSetToString(rs, 3) + "\n";
            } else {
                queryResults += resultSetToString(rs, 3) + "\n";
            }
            rs.close();

            // Execute Query 4: Top 15 items sold
            ps = con.prepareStatement(
                    "SELECT ItemType, SUM(UnitsSold) AS Total_Units_Sold FROM SalesData GROUP BY ItemType ORDER BY Total_Units_Sold DESC LIMIT 15;");
            ps.execute();
            rs = ps.executeQuery();
            if (saveCSV) {
                rawQueryResult += resultSetToJson(rs, 4) + "\n";
                queryResults += resultSetToString(rs, 4) + "\n";
            } else {
                queryResults += resultSetToString(rs, 4) + "\n";
            }
            rs.close();

            // Execute Query 5: Total revenue each year for each region
            ps = con.prepareStatement(
                    "SELECT YEAR(STR_TO_DATE(OrderDate, '%m/%d/%Y')) AS Sales_Year, Region, SUM(TotalRevenue) AS Total_Revenue FROM SalesData GROUP BY Region, Sales_Year ORDER BY Region, Sales_Year ASC, Total_Revenue DESC;");
            rs = ps.executeQuery();
            if (saveCSV) {
                rawQueryResult += resultSetToJson(rs, 5) + "\n";
                queryResults += resultSetToString(rs, 5) + "\n";
            } else {
                queryResults += resultSetToString(rs, 5) + "\n";
            }
            rs.close();
            con.close();
            if (saveCSV) {
                createCSV(bucketname, rawQueryResult, s3Client);
            }
            con.close();
            
            String finalRes = "Database: " + properties.getProperty("database") + 
            " Table:" + properties.getProperty("table") + " processed.\n" +
            "Query Processed: " + queryResults;
            
            return finalRes;
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    public static String createCSV(String bucketname, String result, AmazonS3 s3Client) {
        String fileName = "LQ_results.csv";

        // Create new file on S3
        s3Client.putObject(bucketname, fileName, result);

        return "File created with query result: " + result;
    }

    private static String resultSetToString(ResultSet rs, int number) throws Exception {
        int rowCount = 0;

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            rowCount++;
        }

        return "Query " + number + ": "
                + ", Query processed columns: " + columnCount;
    }

    private static String resultSetToJson(ResultSet rs, int number) throws Exception {
        JsonObject jsonResult = new JsonObject();
        JsonArray columns = new JsonArray();
        JsonArray rows = new JsonArray();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        String queryType = "";
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
        switch (number) {
            case 1:
                queryType = "\nAll data\n";
                break;
            case 2:
                queryType = "\nTotal cost and Total Revenue for each country\n";
                break;
            case 3:
                queryType = "\nCountries with profit revenue above 30%\n";
                break;
            case 4:
                queryType = "\nTop 15 items sold\n";
                break;
            case 5:
                queryType = "\nTotal revenue each year for each region\n";
                break;
        }
        jsonResult.addProperty("Query " + number, queryType);
        jsonResult.add("columns", columns);
        jsonResult.add("rows", rows);
        return jsonResult.toString();
    }

    public HashMap<String, Object> handleRequest(Request request, Context context) {
//        Boolean saveCSV, String bucketname, String filename, 
//        ArrayList<String> headerList, AmazonS3 s3Client

        // Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        // ****************START FUNCTION IMPLEMENTATION*************************
        // Add custom key/value attribute to SAAF's output. (OPTIONAL)
        ArrayList<String> headers = new ArrayList<>();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        String bucketName = request.getBucketname();
        String fileName = "Transform_result.csv";
        //Perform Load-Querry
        String processedQueryResults = performLQ(
                true, 
                bucketName, 
                fileName,
                headers, s3Client);

        // (OPTIONAL)
        LambdaLogger logger = context.getLogger();
        logger.log("test: "+ processedQueryResults);
        Response response = new Response();
        // Set response value
        response.setValue(processedQueryResults);
        inspector.consumeResponse(response);

        // ****************END FUNCTION IMPLEMENTATION***************************
        // Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
