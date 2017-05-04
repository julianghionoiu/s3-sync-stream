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
    private Path privatePropertiesPath;
    private Properties privateProperties;

    public AWSSecretsProvider(Path privatePropertiesPath) {
        privateProperties = loadPrivateProperties(privatePropertiesPath);
        this.privatePropertiesPath = privatePropertiesPath;
    }

    @Override
    public AWSCredentials getCredentials() {
        return new BasicAWSCredentials(
                privateProperties.getProperty("aws_access_key_id"),
                privateProperties.getProperty("aws_secret_access_key"));
    }

    public String getS3Region() {
        return privateProperties.getProperty("s3_region");
    }

    public String getS3Bucket() {
        return privateProperties.getProperty("s3_bucket");
    }

    public String getS3Prefix() {
        return privateProperties.getProperty("s3_prefix");
    }

    @Override
    public void refresh() {
        privateProperties = loadPrivateProperties(privatePropertiesPath);
    }

    //~~~ Util

    private static Properties loadPrivateProperties(Path privatePropertiesPath) {
        Properties properties = new Properties();
        try (InputStream inStream = Files.newInputStream(privatePropertiesPath)) {
            properties.load(inStream);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
