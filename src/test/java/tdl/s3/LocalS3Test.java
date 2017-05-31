package tdl.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.Rule;
import org.junit.Test;
import tdl.s3.rules.LocalS3Server;
import tdl.s3.rules.RemoteTestBucket;

public class LocalS3Test {

    @Rule
    public LocalS3Server localS3 = new LocalS3Server();

    @Test
    public void run() {
        EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");
        AmazonS3 client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withEndpointConfiguration(endpoint)
                .build();

        client.createBucket("testbucket");
        client.putObject("testbucket", "file/name", "contents");
    }
}
