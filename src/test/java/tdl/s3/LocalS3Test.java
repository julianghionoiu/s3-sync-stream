package tdl.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import tdl.s3.rules.LocalS3Server;
import tdl.s3.rules.LocalTestBucket;
import tdl.s3.rules.RemoteTestBucket;

public class LocalS3Test {

    @ClassRule
    public static LocalS3Server localS3 = new LocalS3Server();

    @Rule
    public LocalTestBucket testBucket = new LocalTestBucket();

    @Test
    public void run1() {
        AmazonS3 client = testBucket.getAmazonS3();
        client.createBucket("testbucket");
        client.putObject("testbucket", "file/name", "contents");
    }

    @Test
    public void run2() {
        AmazonS3 client = testBucket.getAmazonS3();
        client.createBucket("testbucket");
        client.putObject("testbucket", "file/name", "contents");
    }

    @Test
    public void run3() {
        AmazonS3 client = testBucket.getAmazonS3();
        client.createBucket("testbucket");
        client.putObject("testbucket", "file/name", "contents");
    }
}
