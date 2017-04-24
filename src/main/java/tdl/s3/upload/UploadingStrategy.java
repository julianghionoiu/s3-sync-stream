package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;

import java.io.File;

public interface UploadingStrategy {

    void upload(AmazonS3 s3, String bucket, String prefix, File file, String newName) throws Exception;
}
