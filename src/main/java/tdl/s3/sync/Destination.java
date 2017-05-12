package tdl.s3.sync;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.nio.file.Path;
import java.nio.file.Paths;
import tdl.s3.credentials.AWSSecretsProvider;
import tdl.s3.upload.RemoteFile;

public class Destination {

    private AWSSecretsProvider secret;

    private AmazonS3 client;

    public static class Builder {

        private final Destination destination = new Destination();

        public Builder loadFromPath(Path path) {
            //TODO: Need to consider removing AWSSecretsProvider class.
            AWSSecretsProvider secrets = new AWSSecretsProvider(path);
            destination.secret = secrets;
            return this;
        }

        public final Destination create() {
            this.destination.buildClient();
            return this.destination;
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    public static Destination createDefaultDestination() {
        Path path = Paths.get(".private/aws-test-secrets");
        return getBuilder()
                .loadFromPath(path)
                .create();
    }

    public AWSSecretsProvider getSecret() {
        return secret;
    }

    public AmazonS3 getClient() {
        return client;
    }

    private void buildClient() {
        this.client = AmazonS3ClientBuilder.standard()
                .withCredentials(secret)
                .withRegion(secret.getS3Region())
                .build();
    }

    public RemoteFile createRemoteFile(String name) {
        return new RemoteFile(secret.getS3Bucket(), secret.getS3Prefix(), name);
    }

    public ObjectMetadata getObjectMetadata(String bucket, String key) {
        return this.client.getObjectMetadata(bucket, key);
    }

    public boolean existsRemoteFile(RemoteFile remoteFile) {
        try {
            client.getObjectMetadata(remoteFile.getBucket(), remoteFile.getFullPath());
            return true;
        } catch (NotFoundException ex) {
            return false;
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return false;
            } else {
                throw ex;
            }
        }
    }
}
