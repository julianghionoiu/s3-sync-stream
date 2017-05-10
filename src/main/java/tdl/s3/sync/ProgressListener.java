package tdl.s3.sync;

import java.io.File;

public interface ProgressListener {

    public void uploadFileStarted(File file, String uploadId);

    public void uploadFileProgress(String uploadId, int uploadedByte);

    public void uploadFileFinished(File file);
}
