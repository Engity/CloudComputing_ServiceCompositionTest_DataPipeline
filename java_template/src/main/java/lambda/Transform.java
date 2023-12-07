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
    public static ArrayList<ArrayList<String>> processData(HashMap<String, ArrayList<String>> rawData, ArrayList<String> headers) {
        int rows = 0;
        int cols = headers.size();

        ArrayList<ArrayList<String>> res = new ArrayList<>();
        res.add(new ArrayList<String>());
        // Initialized the headers
        for (int i = 0 ; i < headers.size(); i++){
            String header = headers.get(i);
            res.get(0).add(header);
            rows = Math.max(rows, rawData.get(header).size());
        }
       
        res.get(0).add("Gross Margin");
        res.get(0).add("Order Processing Time");

        HashSet<String> orderIDSets = new HashSet<>();

        // Parse the records by row
        for (int i = 0; i < rows; i++) {
            Iterator<String> colIterator = headers.iterator();
            // Extract orderID
            String orderID = rawData.get("Order ID").get(i);
            // Only process the record with order id unique
            ArrayList<String> rowRecord = new ArrayList<>();
            if (!orderIDSets.contains(orderID)) {
                orderIDSets.add(orderID);
                // Copy every column
                while (colIterator.hasNext()) {
                    String header = colIterator.next();
                    ArrayList<String> tmp = rawData.get(header);
                    String val = "NULL";
                    if (tmp != null) {
                        if (header.equals("Order Priority")) {
                            switch (tmp.get(i)) {
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
                            val = tmp.get(i);
                    }
                    rowRecord.add(val);
                }

                // Process new data
                // Gross margin
                Double profit = Double.parseDouble(rawData.get("Total Profit").get(i));
                Double revenue = Double.parseDouble(rawData.get("Total Revenue").get(i));
                Double grossMargin = profit / revenue;
                rowRecord.add(grossMargin.toString());

                // Process Date
                SimpleDateFormat myFormat = new SimpleDateFormat("dd/MM/yyyy");
                String inputString1 = rawData.get("Order Date").get(i);
                String inputString2 = rawData.get("Ship Date").get(i);

                try {
                    Date date1 = myFormat.parse(inputString1);
                    Date date2 = myFormat.parse(inputString2);
                    Long diff = (date2.getTime() - date1.getTime());
                    Long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                    rowRecord.add(days.toString());
                } catch (Exception e) {
                    rowRecord.add("INVALID DATE");
                }
            }
            res.add(rowRecord);
        }
        return res;
    }

    public static HashMap<String, ArrayList<String>> parseCSV(InputStream objectData, ArrayList<String> headerList) {
        // Containing the raw data
        HashMap<String, ArrayList<String>> rawData = new HashMap<>();

        Scanner scanner = new Scanner(objectData);

        // Read the headlines
        Scanner lineReader = new Scanner(scanner.nextLine());
        lineReader.useDelimiter(",");
        // Init the headers
        while (lineReader.hasNext()) {
            String header = lineReader.next();
            headerList.add(header);
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
                rawData.get(headerList.get(headerIndex)).add(data);
                headerIndex++;
            }
            lineReader.close();

        }
        scanner.close();
        return rawData;
    }

    public static String createCSV(String bucketname, String fileName, ArrayList<ArrayList<String>> data) {
        int row = 0;
        int col = data.size();

        StringBuilder sw = new StringBuilder();
        for (int i = 0 ; i < data.size(); i++){
            row = Math.max(row, data.get(i).size());
            for (int j = 0 ; j < data.get(i).size(); j++){
                sw.append(data.get(i).get(j));
                if (j + 1 < data.get(i).size()){
                    sw.append(',');
                }
                else{
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
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
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
        // get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));

        // get content of the file
        InputStream objectData = s3Object.getObjectContent();
        ArrayList<String> headers = new ArrayList<>();
        HashMap<String, ArrayList<String>> rawData = parseCSV(objectData, headers);
        // HashMap<String, ArrayList<String>> processData = processData(rawData);
        ArrayList<ArrayList<String>> processData2 = processData(rawData, headers);

        // Create new file in S3
        // String result = createCSV(bucketname, filename.split(".")[0] + "_result.csv",
        // processData);
        // String result = createCSV(bucketname, "Transform_result.csv", processData);
        String result = createCSV(bucketname, "Transform_result.csv", processData2);

        // (OPTIONAL)
        LambdaLogger logger = context.getLogger();
        // logger.log("ProcessCSV bucketname:" + bucketname + " filename:" + filename +
        // " avg-element:" + avg + " total:"
        // + total);
        Response response = new Response();

        // Set response value
        response.setValue("Filename:" + filename + " processed with " + headers.size() + " columns and "
                + rawData.get("Region").size() + " rows.\n"
                + " Result " + result + "\n");

        inspector.consumeResponse(response);

        // ****************END FUNCTION IMPLEMENTATION***************************

        // Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}