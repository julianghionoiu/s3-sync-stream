package tdl.s3.rules;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class LocalTestBucket extends TestBucket {

    public LocalTestBucket() {
        EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");
        amazonS3 = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withEndpointConfiguration(endpoint)
                .build();
        bucketName = "localbucket";
        amazonS3.createBucket(bucketName);
        uploadPrefix = "prefix/";
    }
}
