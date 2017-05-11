package tdl.s3.sync;

import java.io.File;

public interface ProgressListener {

    void uploadFileStarted(File file, String uploadId);

    void uploadFileProgress(String uploadId, long uploadedByte);

    void uploadFileFinished(File file);
}
