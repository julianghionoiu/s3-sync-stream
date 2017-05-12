package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import java.io.File;
import tdl.s3.sync.Destination;

/**
 * General interface to upload any file to S3 storage
 *
 */
public interface FileUploader {

    void upload(File file);

    void upload(File file, RemoteFile remoteFile);

    boolean exists(RemoteFile remoteFile);
}
