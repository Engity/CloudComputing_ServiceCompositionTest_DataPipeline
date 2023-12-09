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
public class SwitchBoard implements RequestHandler<Request, HashMap<String, Object>> {
    // Process the data as according to the transform guideline in the project
    // Remove duplicated Order ID
    // Create two new columns Gross Margin and Order Processing Time
    // Transform Order Priority
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

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
        String result = "";
        Integer operationType = request.getOperationtype();
        switch (operationType) {
            case 1:
                // Perform transform
                ArrayList<ArrayList<String>> transformRes = Transform.performTransform(bucketname, filename, s3Client,
                        true);

                result = "Performing Transform with Processed Rows=" + transformRes.size() + " Cols=" + transformRes.get(0).size();
                break;
            case 2:
                result = "Performing Load " + LoadData.PerformLoad(bucketname, "Transform_result.csv", s3Client);
                break;
            case 3:
                // Perform Query
                result = "Performing Query " + Query.performQuery(true, bucketname, s3Client);
                break;
            case 4:
                // Perform Transform Load
                result = "Performing TL " + TLService.PerformTL(bucketname,filename, s3Client);
                break;
            case 5:
                // Perform Load Query
                result = "Performing LQ";
                break;
            case 6:
                // Perform TLQ
                result = "Performing TLQ";
                break;
        }

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
}