package tdl.s3.upload;

import java.io.File;

/**
 * General interface to upload any file to S3 storage
 *
 *
 * @author vdanyliuk
 * @version  11.04.17.
 */
public interface FileUploader {

    void upload(File file);

    void upload(File file, String newName);

    boolean exists(String bucketName, String fileKey);
}
