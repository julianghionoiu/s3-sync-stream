package tdl.s3.sync.progress;

import java.io.File;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class UploadStatsProgressListener implements ProgressListener {

    public static class FileUploadStat {

        private final double BYTE_PER_MILLISECOND_TO_MEGABYTES_PER_SECOND = 0.001;

        private long totalSize = 0;
        
        private final AtomicLong uploadedSize;

        private long startTimestamp = 0;

        FileUploadStat(long totalSize, long uploadedByte) {
            this.totalSize = totalSize;
            this.startTimestamp = new Date().getTime();
            this.uploadedSize = new AtomicLong(uploadedByte);
        }

        public long getTotalSize() {
            return totalSize;
        }

        public long getUploadedSize() {
            return uploadedSize.get();
        }

        void incrementUploadedSize(long size) {
            this.uploadedSize.getAndAdd(size);
        }

        public double getMBps() {
            double elapsedMilliseconds = (new Date().getTime() - this.startTimestamp);
            if (elapsedMilliseconds == 0) {
                return 0;
            }
            double bytesUploaded = (double) this.uploadedSize.get();
            double bytePerMillisecond = bytesUploaded / elapsedMilliseconds;
            return bytePerMillisecond * BYTE_PER_MILLISECOND_TO_MEGABYTES_PER_SECOND;
        }

        public double getUploadRatio() {
            return (double) uploadedSize.get() / (double) totalSize;
        }
    }

    private FileUploadStat fileUploadStat = null;

    @Override
    public void uploadFileStarted(File file, String uploadId, long uploadedByte) {
        fileUploadStat = new FileUploadStat(file.length(), uploadedByte);
    }

    @Override
    public void uploadFileProgress(String uploadId, long uploadedByte) {
        fileUploadStat.incrementUploadedSize(uploadedByte);
    }

    @Override
    public void uploadFileFinished(File file) {
        fileUploadStat = null;
    }


    //~~~~ Getters


    public Optional<FileUploadStat> getCurrentStats() {
        //TODO Improve this class so that it can handle multiple uploads simultaneously
        return Optional.ofNullable(fileUploadStat);
    }


    public boolean isCurrentlyUploading() {
        return fileUploadStat != null;
    }
}
