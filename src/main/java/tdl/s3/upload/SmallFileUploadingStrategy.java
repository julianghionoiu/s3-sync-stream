package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import lombok.extern.slf4j.Slf4j;

import java.io.File;



/**
 * @author vdanyliuk
 * @version 11.04.17.
 */
@Slf4j
public class SmallFileUploadingStrategy implements UploadingStrategy {

    @Override
    public void upload(AmazonS3 s3, String bucket, File file, String newName) {
        log.debug("Uploading file " + file + " with SmallFileUploadingStrategy.");
        s3.putObject(bucket, newName, file);
    }

}
