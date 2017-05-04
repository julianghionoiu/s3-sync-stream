package tdl.s3.credentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Read credentials and bucket information from private properties file.
 *
 * The file should contain the following keys:
 *  - aws_access_key_id
 *  - aws_secret_access_key
 *  - s3_region
 *  - s3_bucket
 */
public class AWSSecretsProvider implements AWSCredentialsProvider {
    
    private Properties properties;

    public AWSSecretsProvider(Properties properties) {
        this.properties = properties;
    }

    @Override
    public AWSCredentials getCredentials() {
        return new BasicAWSCredentials(
            properties.getProperty("aws_access_key_id"),
            properties.getProperty("aws_secret_access_key")
        );
    }

    public String getS3Region() {
        return properties.getProperty("s3_region");
    }

    public String getS3Bucket() {
        return properties.getProperty("s3_bucket");
    }

    public String getS3Prefix() {
        return properties.getProperty("s3_prefix");
    }

    @Override
    public void refresh() {
        //TODO set the app to reload the properties from file.
        //Maybe pass the app into the constructor.
    }
}
