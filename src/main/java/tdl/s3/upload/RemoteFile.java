package tdl.s3.upload;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;

/**
 * This represents a single S3 object.
 */
public class RemoteFile {

    private final String bucket;

    private final String prefix;

    private final String path;

    private AmazonS3 client;

    public RemoteFile(String bucket, String path) {
        this.bucket = bucket;
        this.path = path;
        this.prefix = "";
    }

    public RemoteFile(String bucket, String prefix, String path) {
        this.bucket = bucket;
        this.prefix = prefix;
        this.path = path;
        this.client = createDefaultClient();
    }

    public RemoteFile(String bucket, String prefix, String path, AmazonS3 client) {
        this.bucket = bucket;
        this.prefix = prefix;
        this.path = path;
        this.client = client;
    }

    public void setClient(AmazonS3 client) {
        this.client = client;
    }

    private AmazonS3 createDefaultClient() {
        //TODO: Create based on instance profile or environment.
        return null;
    }
    
    public String getPrefix() {
        return prefix;
    }

    public String getBucket() {
        return bucket;
    }

    public String getFullPath() {
        return prefix + path;
    }

    public boolean exists() {
        try {
            client.getObjectMetadata(bucket, getFullPath());
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
