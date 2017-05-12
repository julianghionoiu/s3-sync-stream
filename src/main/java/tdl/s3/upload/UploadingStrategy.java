package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;

import java.io.File;
import tdl.s3.sync.ProgressListener;

public interface UploadingStrategy {
    
    void setClient(AmazonS3 client);

    void upload(File file, RemoteFile remoteFile) throws Exception;

    void setListener(ProgressListener listener);
}
