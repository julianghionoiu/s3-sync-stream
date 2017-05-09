package tdl.s3.upload;

import java.io.File;
import tdl.s3.sync.SyncProgressListener;

/**
 * General interface to upload any file to S3 storage
 *
 */
public interface FileUploader {

    void upload(File file);

    void upload(File file, RemoteFile remoteFile);

    boolean exists(RemoteFile remoteFile);
}
