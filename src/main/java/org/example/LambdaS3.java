package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class LambdaS3 implements RequestHandler<S3Event, String> {
    @Override
    public String handleRequest(S3Event s3event, Context context) {
        // Initialize the Amazon S3 client
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        try {
            // Get the record from the event
            S3EventNotification.S3EventNotificationRecord record = s3event.getRecords().get(0);

            // Extract bucket name and object key
            String bucketName = record.getS3().getBucket().getName();
            String objectKey = record.getS3().getObject().getKey().replace('+', ' ');

            context.getLogger().log("Bucket: " + bucketName + ", Key: " + objectKey);

            // Get the object from S3
            S3Object s3Object = s3Client.getObject(bucketName, objectKey);

            // Read the content (assuming it's a text file)
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Process each line (for demonstration, we're just logging it)
                    context.getLogger().log(line);
                }
            }

            return "Processed object: " + objectKey;

        } catch (Exception e) {
            context.getLogger().log("Error processing S3 event: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
