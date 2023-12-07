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
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    public static ArrayList<ArrayList<String>> processData(ArrayList<ArrayList<String>> rawData, ArrayList<String> headers) {
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
                for (int j = 0; j < cols; j++){
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

                // Process Date
                SimpleDateFormat myFormat = new SimpleDateFormat("dd/MM/yyyy");
                String inputString1 = rawData.get(i).get(5);
                String inputString2 = rawData.get(i).get(7);

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
            ArrayList<String> tmp  = new ArrayList<>();
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
       
        // get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));

        // get content of the file
        InputStream objectData = s3Object.getObjectContent();
        ArrayList<String> headers = new ArrayList<>();
        ArrayList<ArrayList<String>> rawData = parseCSV(objectData, headers);
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
                + rawData.get(0).size() + " rows.\n"
                + " Result " + result + "\n");

        inspector.consumeResponse(response);

        // ****************END FUNCTION IMPLEMENTATION***************************

        // Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}