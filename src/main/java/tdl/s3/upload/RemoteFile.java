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

    public RemoteFile(String bucket, String prefix, String path) {
        this.bucket = bucket;
        this.prefix = prefix;
        this.path = path;
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
}
