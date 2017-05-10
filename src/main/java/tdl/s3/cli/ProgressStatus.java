package tdl.s3.cli;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import tdl.s3.sync.ProgressListener;

public class ProgressStatus implements ProgressListener {

    public static class FileStat {

        private int totalSize = 0;

        private int uploadedSize = 0;

        public FileStat(int totalSize) {
            this.totalSize = totalSize;
        }

        public int getTotalSize() {
            return totalSize;
        }

        public int getUploadedSize() {
            return uploadedSize;
        }

        public void incrementUploadedSize(int size) {
            this.uploadedSize += size;
        }
    }

    private Map<String, FileStat> fileStats = new HashMap<>();

    @Override
    public void uploadFileStarted(File file, String uploadId) {
        System.out.println("Uploading file: " + file);
        FileStat stat = new FileStat((int) file.length());
        fileStats.put(uploadId, stat);
    }

    @Override
    public void uploadFileProgress(String uploadId, int uploadedByte) {
        FileStat stat = fileStats.get(uploadId);
        stat.incrementUploadedSize(uploadedByte);
        System.out.println("Uploaded " + stat.getUploadedSize() + "/" + stat.getTotalSize() + " bytes");
    }

    @Override
    public void uploadFileFinished(File file) {
        System.out.println("Finished");
    }

}
