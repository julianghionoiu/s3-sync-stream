package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Date;

/**
 * * @deprecated,
 * replaced by {@link MultiPartUploadFileUploadingStrategy}
 */
@Slf4j
@Deprecated
public class LargeFileUploadingStrategy implements UploadingStrategy {

    @Override
    public void upload(AmazonS3 s3, String bucket, String prefix, File file, String newName) throws Exception {
        log.debug("Uploading file " + file + " with LargeFileUploadingStrategy.");
        TransferManager transferManager = TransferManagerBuilder
                .standard()
                .withS3Client(s3)
                .build();
        Upload upload = transferManager.upload(bucket, prefix + newName, file);
        upload.waitForCompletion();
        new Date(2015, 1, 12);
        transferManager.shutdownNow(false);
    }

}
