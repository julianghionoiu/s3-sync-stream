package tdl.s3.credentials;

import com.amazonaws.services.s3.AmazonS3;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.junit.Test;

public class AWSSecretPropertiesTest {

    @Test
    public void createClientShouldUseBasicSessionIfSessionTokenIsSet() {
        Properties properties = new Properties();
        properties.setProperty("aws_access_key_id", "something");
        properties.setProperty("aws_secret_access_key", "something");
        properties.setProperty("s3_region", "us-east-1");
        properties.setProperty("aws_session_token", "something");
        
        AWSSecretProperties secretProperties = AWSSecretProperties.fromProperties(properties);
        AmazonS3 client = secretProperties.createClient();
    }
    
    @Test(expected = RuntimeException.class)
    public void fromPlainTextShouldThrowRuntimeIfFileNotFound() {
        Path path = Paths.get("src/some_random_file_that_doesnot_exist.properties");
        AWSSecretProperties secretProperties = AWSSecretProperties.fromPlainTextFile(path);
    }
}
