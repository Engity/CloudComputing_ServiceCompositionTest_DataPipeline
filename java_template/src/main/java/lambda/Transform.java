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
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 * @author Toan Nguyen
 */
public class Transform implements RequestHandler<Request, HashMap<String, Object>> {
    // Process the data as according to the transform guideline in the project
    // Remove duplicated Order ID
    // Create two new columns Gross Margin and Order Processing Time
    // Transform Order Priority
    public static ArrayList<ArrayList<String>> processData(ArrayList<ArrayList<String>> rawData,
            ArrayList<String> headers) {
        int rows = rawData.size();
        int cols = headers.size();

        ArrayList<ArrayList<String>> res = new ArrayList<>();
        res.add(rawData.get(0));
        // Initialized the headers
        res.get(0).add("Gross Margin");
        res.get(0).add("Order Processing Time");

        HashSet<String> orderIDSets = new HashSet<>();

        // Parse the records by row
        for (int i = 1; i < rows; i++) {
            // Extract orderID
            String orderID = rawData.get(i).get(6);
            // Only process the record with order id unique
            ArrayList<String> rowRecord = new ArrayList<>();

            if (!orderIDSets.contains(orderID)) {
                orderIDSets.add(orderID);
                // Copy every column
                for (int j = 0; j < cols; j++) {
                    ArrayList<String> tmp = rawData.get(i);
                    String val = "NULL";
                    if (tmp != null) {
                        if (headers.get(j).equals("Order Priority")) {
                            switch (tmp.get(j)) {
                                case "L":
                                    val = "Low";
                                    break;
                                case "M":
                                    val = "Medium";
                                    break;
                                case "H":
                                    val = "High";
                                    break;
                                case "C":
                                    val = "Critical";
                                    break;
                            }
                        } else
                            val = tmp.get(j);
                    }
                    rowRecord.add(val);
                }

                // Process new data
                // Gross margin
                Double profit = Double.parseDouble(rawData.get(i).get(13));
                Double revenue = Double.parseDouble(rawData.get(i).get(11));
                Double grossMargin = profit / revenue;
                rowRecord.add(grossMargin.toString());
                // Australia and Oceania,Tuvalu,Baby
                // Food,Offline,High,5/28/2010,669165933,6/27/2010,9925,255.28,159.42,2533654.00,1582243.50,951410.50,0.3755092447508618,0
                // -30Thu Apr 05 00:00:00 UTC 2012 Tue Mar 06 00:00:00 UTC 2012
                // Process Date
                // 0 -30Thu Apr 05 00:00:00 UTC 2012 Tue Mar 06 00:00:00 UTC 2012
                SimpleDateFormat myFormat = new SimpleDateFormat("MM/dd/yyyy");
                String inputString1 = rawData.get(i).get(5);
                String inputString2 = rawData.get(i).get(7);

                try {
                    Date date1 = myFormat.parse(inputString1);
                    Date date2 = myFormat.parse(inputString2);
                    Long diff = (date2.getTime() - date1.getTime()) / 86400000;
                    rowRecord.add(diff.toString());
                } catch (Exception e) {
                    rowRecord.add("INVALID DATE");
                }
            }
            res.add(rowRecord);
        }
        return res;
    }

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

    public static String createCSV(String bucketname, String fileName, ArrayList<ArrayList<String>> data,
            AmazonS3 s3Client) {
        int row = 0;
        int col = data.size();

        StringBuilder sw = new StringBuilder();
        for (int i = 0; i < data.size(); i++) {
            row = Math.max(row, data.get(i).size());
            for (int j = 0; j < data.get(i).size(); j++) {
                sw.append(data.get(i).get(j));
                if (j + 1 < data.get(i).size()) {
                    sw.append(',');
                } else {
                    sw.append('\n');
                }
            }
        }

        // Format toread the data
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);

        meta.setContentType("text/plain");
        // Create new file on S3

        s3Client.putObject(bucketname, fileName, is, meta);

        return "File created with cols: " + col + " rows: " + row;
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

        // ****************START FUNCTION IMPLEMENTATION*************************
        // Add custom key/value attribute to SAAF's output. (OPTIONAL)

        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        ArrayList<ArrayList<String>> transformRes = performTransform(bucketname, filename, s3Client, true);
        String result = "Processed Rows=" + transformRes.size() + " Cols=" + transformRes.get(0).size();

        // (OPTIONAL)
        LambdaLogger logger = context.getLogger();
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

    public static ArrayList<ArrayList<String>> performTransform(String bucketname, String filename, AmazonS3 s3Client,
            boolean saveCSV) {
        // get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));

        // get content of the file
        InputStream objectData = s3Object.getObjectContent();
        ArrayList<String> headerList = new ArrayList<>();
        // ArrayList<ArrayList<String>> rawData = parseCSV(objectData, headerList);
        // HashMap<String, ArrayList<String>> processData = processData(rawData);
        // ArrayList<ArrayList<String>> processData2 = processData(rawData, headerList);

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
            if (saveCSV)
                sw.append(header).append(',');
        }

        headerList.add("Gross Margin");
        headerList.add("Order Processing Time");
        if (saveCSV)
            sw.append("Gross Margin,Order Processing Time\n");
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
                if (saveCSV)
                    // copy to sw to write to csv
                    for (int i = 0; i < tmp.size(); i++) {
                        sw.append(tmp.get(i));
                        if (i + 1 < tmp.size()) {
                            sw.append(',');
                        } else {
                            sw.append("\n");
                        }
                    }
                rawData.add(tmp);
            }

            lineReader.close();

        }
        scanner.close();
        if (saveCSV) {
            byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
            InputStream is = new ByteArrayInputStream(bytes);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);

            meta.setContentType("text/plain");
            // Create new file on S3

            s3Client.putObject(bucketname, "Transform_result.csv", is, meta);
        }

        String finalRes = "Filename:" + filename + " processed with " + headerList.size() + " columns and "
                + rawData.get(0).size() + " rows.\n";
        return rawData;
    }
}