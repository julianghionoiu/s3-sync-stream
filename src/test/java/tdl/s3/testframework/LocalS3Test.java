package tdl.s3.testframework;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tdl.s3.testframework.rules.LocalTestBucket;

import static org.junit.Assert.assertNotNull;

public class LocalS3Test {

    public LocalTestBucket testBucket;

    @BeforeEach
    void setUp() {
        testBucket = new LocalTestBucket();
        testBucket.beforeEach();
    }

    @Test
    public void can_use_minio_server_correctly() {
        AmazonS3 client = testBucket.getAmazonS3();
        String testbucket = "testbucket";
        if (!client.doesBucketExist(testbucket)) {
            client.createBucket(testbucket);
        }
        client.putObject(testbucket, "file/name", "contents");
        ObjectMetadata data = client.getObjectMetadata(testbucket, "file/name");
        Assertions.assertNotNull(data.getETag());
    }
}
