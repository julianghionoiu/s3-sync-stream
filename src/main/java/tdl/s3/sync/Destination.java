package tdl.s3.sync;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import tdl.s3.credentials.AWSSecretsProvider;

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
    
    public String getBucket() {
        return secret.getS3Bucket();
    }
    
    public String getPrefix() {
        return secret.getS3Prefix();
    }

    private void buildClient() {
        this.client = AmazonS3ClientBuilder.standard()
                .withCredentials(secret)
                .withRegion(secret.getS3Region())
                .build();
    }

    public ObjectMetadata getObjectMetadata(String bucket, String key) {
        return this.client.getObjectMetadata(bucket, key);
    }
    
    public String getFullPath(String path) {
        return getPrefix()+ path;
    }

    public boolean canUpload(String path) {
        try {
            client.getObjectMetadata(getBucket(), getFullPath(path));
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
    
    public String initUploading(String remotePath) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(getBucket(), getFullPath(remotePath));
        InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
        return result.getUploadId();
    }
    
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) {
        return client.listMultipartUploads(request);
    }
    
    public PartListing listParts(ListPartsRequest request) {
        return client.listParts(request);
    }

    public UploadPartResult uploadPart(UploadPartRequest request)  {
        return client.uploadPart(request);
    }
    
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        return client.completeMultipartUpload(request);
    }
}
