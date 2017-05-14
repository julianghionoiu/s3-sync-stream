package tdl.s3.upload;


import java.io.File;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.progress.ProgressListener;

public interface UploadingStrategy {
    
    void setDestination(Destination destination);

    void upload(File file, String remotePath) throws Exception;

    void setListener(ProgressListener listener);
}
