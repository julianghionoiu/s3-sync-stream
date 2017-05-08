package tdl.s3.cli;

import tdl.s3.sync.SyncProgressListener;

public class ProgressBar implements SyncProgressListener {

    private final int width = 100;

    private int totalSizeBytes = 0;

    public void updateProgress(double percentage) {
        System.out.print("\r[");
        int i = 0;
        for (; i <= (int) (percentage * width); i++) {
            System.out.print(".");
        }
        for (; i < width; i++) {
            System.out.print(" ");
        }
        System.out.print("]");
    }

    //TODO: set Listener
    @Override
    public void uploadStarted(String uploadId, int numFiles, int numParts, int totalSizeBytes) {
        this.totalSizeBytes = totalSizeBytes;
    }

    @Override
    public void uploadProgressChanged(String uploadId, int bytesTransfered) {
        double percentage = bytesTransfered / totalSizeBytes;
        updateProgress(percentage);
    }

    @Override
    public void uploadComplete(String uploadId) {
        System.out.println("");
    }
}
