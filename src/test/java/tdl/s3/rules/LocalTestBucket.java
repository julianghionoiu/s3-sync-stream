package tdl.s3.rules;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class LocalTestBucket extends TestBucket {

    public LocalTestBucket() {
        EndpointConfiguration endpoint = new EndpointConfiguration("http://127.0.0.1:9000", "us-east-1");
        AWSCredentials credential = new BasicAWSCredentials(
                "minio_access_key",
                "minio_secret_key"
        );
        amazonS3 = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(credential))
                .withEndpointConfiguration(endpoint)
                .build();
        bucketName = "localbucket";
        if (!amazonS3.doesBucketExist(bucketName)) {
            amazonS3.createBucket(bucketName);
        }
        uploadPrefix = "prefix/";
    }
}
