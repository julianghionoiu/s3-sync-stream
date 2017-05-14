package tdl.s3.sync.progress;

import java.io.File;

public interface ProgressListener {

    void uploadFileStarted(File file, String uploadId);

    void uploadFileProgress(String uploadId, long uploadedByte);

    void uploadFileFinished(File file);
}
