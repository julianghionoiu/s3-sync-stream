package tdl.s3.upload;

import java.io.File;

/**
 * General interface to upload any file to S3 storage
 *
 */
public interface FileUploader {

    void upload(File file);

    void upload(File file, String newName);

    boolean exists(String bucketName, String fileKey);
}
