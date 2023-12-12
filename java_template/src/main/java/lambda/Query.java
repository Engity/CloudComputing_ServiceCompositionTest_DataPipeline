package lambda;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Properties;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import saaf.Inspector;
import saaf.Response;

public class Query implements RequestHandler<Request, HashMap<String, Object>> {
    public static String performDelete() {
        // Load data.
        Properties properties = new Properties();
        try {

            properties.load(new FileInputStream("db.properties"));

            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            Connection con = DriverManager.getConnection(url, username, password);
            String queryResults ="Dropped Table and Created Table";
           
            
            // Execute Query 1: All data from database
            PreparedStatement ps = con.prepareStatement("DROP TABLE SalesData;");
            ps.executeUpdate();
            

            // Execute Query 2: Total cost and Total Revenue for each country
            ps = con.prepareStatement(
                    "CREATE TABLE SalesData (\n" + //
                            "    ID INT AUTO_INCREMENT PRIMARY KEY,\n" + //
                            "    Region VARCHAR(255),\n" + //
                            "    Country VARCHAR(255),\n" + //
                            "    ItemType VARCHAR(255),\n" + //
                            "    SalesChannel VARCHAR(255),\n" + //
                            "    OrderPriority VARCHAR(10),\n" + //
                            "    OrderDate VARCHAR(100),\n" + //
                            "    OrderID VARCHAR(100),\n" + //
                            "    ShipDate VARCHAR(100),\n" + //
                            "    UnitsSold INT,\n" + //
                            "    UnitPrice DOUBLE,\n" + //
                            "    UnitCost DOUBLE,\n" + //
                            "    TotalRevenue DOUBLE,\n" + //
                            "    TotalCost DOUBLE,\n" + //
                            "    TotalProfit DOUBLE,\n" + //
                            "    GrossMargin FLOAT,\n" + //
                            "    OrderProcessingTime INT\n" + //
                            ");");
            ps.executeUpdate();
        
            

           
            return queryResults;
        } catch (Exception e) {
            System.out.println("Loi ne "+e.getMessage());
            return null;
        }
    }
    public static String performQuery(Boolean saveCSV, String bucketname, AmazonS3 s3Client) {
        // Load data.
        Properties properties = new Properties();
        try {

            properties.load(new FileInputStream("db.properties"));

            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            Connection con = DriverManager.getConnection(url, username, password);
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
            // Set the formatted results into the response
            con.close();
            return queryResults;
        } catch (Exception e) {
            return null;
        }
    }

    public static String createCSV(String bucketname, String result, AmazonS3 s3Client) {
        String fileName = "Querry_results.csv";

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
                + "Query processed columns: " + columnCount;
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

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        Response response = new Response();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        String bucketName = request.getBucketname();
        String processedQueryResults = performQuery(true, bucketName, s3Client);
        
        
        logger.log("Query Results: " + processedQueryResults);
        response.setValue(processedQueryResults);
        inspector.consumeResponse(response);
        // ****************END FUNCTION IMPLEMENTATION***************************
        // Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
