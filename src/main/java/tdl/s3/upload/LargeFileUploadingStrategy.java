package tdl.s3.upload;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * @author vdanyliuk
 * @version 11.04.17.
 */
@Slf4j
public class LargeFileUploadingStrategy implements UploadingStrategy {

    @Override
    public void upload(AmazonS3 s3, String bucket, File file, String newName) throws Exception {
        log.debug("Uploading file " + file + " with LargeFileUploadingStrategy.");
        TransferManager transferManager = TransferManagerBuilder
                .standard()
                .withS3Client(s3)
                .build();
        Upload upload = transferManager.upload(bucket, newName, file);
        upload.waitForCompletion();
        transferManager.shutdownNow(false);
    }

}
