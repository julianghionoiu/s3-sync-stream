package tdl.s3.sync;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.nio.file.Path;
import java.nio.file.Paths;
import tdl.s3.credentials.AWSSecretsProvider;

public class Destination {

    private AWSSecretsProvider secret;

    public static class Builder {

        private final Destination destination = new Destination();

        public Builder loadFromPath(Path path) {
            //TODO: Need to consider removing AWSSecretsProvider class.
            AWSSecretsProvider secrets = new AWSSecretsProvider(path);
            destination.secret = secrets;
            return this;
        }

        public final Destination create() {
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

    public AmazonS3 buildClient() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(secret)
                .withRegion(secret.getS3Region())
                .build();
    }
}
