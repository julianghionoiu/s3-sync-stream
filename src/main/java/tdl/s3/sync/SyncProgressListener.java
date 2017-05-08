package tdl.s3.sync;

public interface SyncProgressListener {

    public void uploadStarted(String uploadId, int numFiles, int numParts, int totalSizeBytes);

    public void uploadProgressChanged(String uploadId, int bytesTransfered);

    public void uploadComplete(String uploadId);
}
