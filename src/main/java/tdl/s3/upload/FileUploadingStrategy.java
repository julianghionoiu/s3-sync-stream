package tdl.s3.upload;

import java.io.File;
import java.io.IOException;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;
import tdl.s3.sync.progress.ProgressListener;

public interface FileUploadingStrategy {

    void upload(File file, String remotePath) throws DestinationOperationException, IOException;

    void setListener(ProgressListener listener);
}
