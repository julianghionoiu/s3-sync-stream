package tdl.s3.testframework;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Rule;
import org.junit.Test;
import tdl.s3.testframework.rules.LocalTestBucket;

import static org.junit.Assert.*;

public class LocalS3Test {

    @Rule
    public LocalTestBucket testBucket = new LocalTestBucket();

    @Test
    public void can_use_minio_server_correctly() {
        AmazonS3 client = testBucket.getAmazonS3();
        String testbucket = "testbucket";
        if (!client.doesBucketExist(testbucket)) {
            client.createBucket(testbucket);
        }
        client.putObject(testbucket, "file/name", "contents");
        ObjectMetadata data = client.getObjectMetadata(testbucket, "file/name");
        assertNotNull(data.getETag());
    }
}
