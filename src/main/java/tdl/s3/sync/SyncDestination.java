package tdl.s3.sync;

import java.nio.file.Path;
import tdl.s3.credentials.AWSSecretsProvider;

public class SyncDestination {
    
    private String bucket;
    
    private String prefix = "";
    
    private AWSSecretsProvider secret;

    public static class Builder {
        
        private final SyncDestination destination = new SyncDestination();

        public Builder(String bucket) {
            destination.bucket = bucket;
        }
     
        public Builder withCredentials(Path path) {
            return this;
        }
        
        public Builder setBucket(String bucket) {
            destination.bucket = bucket;
            return this;
        }
        
        public Builder setPrefix(String prefix) {
            destination.prefix = prefix;
            return this;
        }
        
        public SyncDestination create() {
            return this.destination;
        }
    }
    
    public static Builder getBuilder(String bucket) {
        return new Builder(bucket);
    }

    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public AWSSecretsProvider getSecret() {
        return secret;
    }
}
