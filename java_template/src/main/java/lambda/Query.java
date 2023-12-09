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
import java.io.ByteArrayInputStream;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import saaf.Inspector;
import saaf.Response;

import java.util.HashMap;
import java.util.Properties;

public class Query implements RequestHandler<Request, HashMap<String, Object>> {

    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    public HashMap<String, String> performQuery() {
        // Load data.
        Properties properties = new Properties();
        try {

            properties.load(new FileInputStream("db.properties"));

            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            Connection con = DriverManager.getConnection(url, username, password);
            HashMap<String, String> queryResults = new HashMap<>();
            // Execute Query 1: All data from database
            PreparedStatement ps = con.prepareStatement(
                    "SELECT * FROM SalesData;");
            ResultSet rs = ps.executeQuery();
            queryResults.put("formattedJson_1", resultSetToJson(rs, 1));
            queryResults.put("formattedString_1", resultSetToString(rs, 1));
            rs.close();

            // Execute Query 2: Total cost and Total Revenue for each country
            ps = con.prepareStatement(
                    "SELECT Country, SUM(TotalRevenue) AS Total_Revenue, SUM(TotalCost) AS Total_Cost FROM SalesData GROUP BY Country;");
            ps.execute();
            rs = ps.executeQuery();
            queryResults.put("formattedJson_2", resultSetToJson(rs, 2));
            queryResults.put("formattedString_2", resultSetToString(rs, 2));
            rs.close();

            // Execute Query 3: Countries with profit revenue above 30%
            ps = con.prepareStatement(
                    "SELECT Country, AVG((TotalProfit / TotalRevenue) * 100) AS Profit_Margin FROM SalesData GROUP BY Country HAVING Profit_Margin > 30;");
            ps.execute();
            rs = ps.executeQuery();
            queryResults.put("formattedJson_3", resultSetToJson(rs, 3));
            queryResults.put("formattedString_3", resultSetToString(rs, 3));
            rs.close();

            // Execute Query 4: Top 15 items sold
            ps = con.prepareStatement(
                    "SELECT ItemType, SUM(UnitsSold) AS Total_Units_Sold FROM SalesData GROUP BY ItemType ORDER BY Total_Units_Sold DESC LIMIT 15;");
            ps.execute();
            rs = ps.executeQuery();
            queryResults.put("formattedJson_4", resultSetToJson(rs, 4));
            queryResults.put("formattedString_4", resultSetToString(rs, 4));
            rs.close();

            // Execute Query 5: Total revenue each year for each region
            ps = con.prepareStatement(
                    "SELECT YEAR(STR_TO_DATE(OrderDate, '%m/%d/%Y')) AS Sales_Year, Region, SUM(TotalRevenue) AS Total_Revenue FROM SalesData GROUP BY Region, Sales_Year ORDER BY Region, Sales_Year ASC, Total_Revenue DESC;");
            rs = ps.executeQuery();
            queryResults.put("formattedJson_5", resultSetToJson(rs, 5));
            queryResults.put("formattedString_5", resultSetToString(rs, 5));
            rs.close();

            con.close();
            // Set the formatted results into the response
            con.close();
            return queryResults;
        } catch (Exception e) {
            return null;
        }
    }

    public static String createCSV(String bucketname, String result) {
        String fileName = "Querry_results.csv";

        // Create new file on S3
        s3Client.putObject(bucketname, fileName, result);

        return "File created with query result: " + result;
    }

    private String resultSetToString(ResultSet rs, int number) throws Exception {
        int rowCount = 0;

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            rowCount++;
        }

        return "Query " + number + ": "
                + "Query processed rows: " + rowCount
                + ", Query processed columns: " + columnCount;
    }

    private String resultSetToJson(ResultSet rs, int number) throws Exception {
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

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        Response response = new Response();

        String bucketName = request.getBucketname();
        HashMap<String, String> queryResults = performQuery();

        String rawQueryResult = "";
        String processedQueryResult = "";

        for (String key : queryResults.keySet()) {
            if (key.startsWith("formattedJson")) {
                rawQueryResult += queryResults.get(key) + "\n";
            } else if (key.startsWith("formattedString")) {
                processedQueryResult += queryResults.get(key) + "\n";
            }
        }

        createCSV(bucketName, rawQueryResult);
        logger.log("Query Results: " + processedQueryResult);
        response.setValue(processedQueryResult);
        inspector.consumeResponse(response);
        // ****************END FUNCTION IMPLEMENTATION***************************
        // Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
