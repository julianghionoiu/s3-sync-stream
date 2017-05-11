package tdl.s3.cli;

import java.io.File;
import java.text.NumberFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import tdl.s3.sync.ProgressListener;

public class ProgressStatus implements ProgressListener {

    private static final NumberFormat percentageFormatter = NumberFormat.getPercentInstance();

    private static final NumberFormat uploadSpeedFormatter = NumberFormat.getNumberInstance();

    static {
        percentageFormatter.setMinimumFractionDigits(1);
        uploadSpeedFormatter.setMinimumFractionDigits(1);
    }

    public static class FileUploadStat {

        private final double BYTE_PER_MILLISECOND_TO_MEGABYTES_PER_SECOND = 0.001;

        private int totalSize = 0;

        private long uploadedSize = 0;

        private long startTimestamp = 0;

        FileUploadStat(int totalSize) {
            this.totalSize = totalSize;
            this.startTimestamp = new Date().getTime();
        }

        int getTotalSize() {
            return totalSize;
        }

        long getUploadedSize() {
            return uploadedSize;
        }

        void incrementUploadedSize(long size) {
            this.uploadedSize += size;
        }

        double getMBps() {
            double elapsedMilliseconds = (new Date().getTime() - this.startTimestamp);
            if (elapsedMilliseconds == 0) {
                return 0;
            }
            double bytesUploaded = (double) this.uploadedSize;
            double bytePerMillisecond = bytesUploaded / elapsedMilliseconds;
            return bytePerMillisecond * BYTE_PER_MILLISECOND_TO_MEGABYTES_PER_SECOND;
        }

        double getUploadRatio() {
            return (double) uploadedSize / (double) totalSize;
        }
    }

    private Map<String, FileUploadStat> fileStats = new HashMap<>();

    @Override
    public void uploadFileStarted(File file, String uploadId) {
        System.out.println("Uploading file: " + file);
        FileUploadStat stat = new FileUploadStat((int) file.length());
        fileStats.put(uploadId, stat);
    }

    @Override
    public void uploadFileProgress(String uploadId, long uploadedByte) {
        FileUploadStat stat = fileStats.get(uploadId);
        stat.incrementUploadedSize(uploadedByte);
        System.out.print("\rUploaded : "
                + percentageFormatter.format(stat.getUploadRatio())
                + ". "
                + stat.getUploadedSize() + "/" + stat.getTotalSize()
                + " bytes. "
                + uploadSpeedFormatter.format(stat.getMBps())
                + " Mbps\t\t\t"
        );
    }

    @Override
    public void uploadFileFinished(File file) {
        System.out.println("\nFinished");
    }

}
