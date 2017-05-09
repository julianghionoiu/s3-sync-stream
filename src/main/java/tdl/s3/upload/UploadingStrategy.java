package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;

import java.io.File;
import tdl.s3.sync.SyncProgressListener;

public interface UploadingStrategy {

    void upload(AmazonS3 s3, File file, RemoteFile remoteFile) throws Exception;

    void setListener(SyncProgressListener listener);
}
