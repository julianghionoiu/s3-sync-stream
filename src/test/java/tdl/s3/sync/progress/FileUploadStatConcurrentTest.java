package tdl.s3.sync.progress;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class FileUploadStatConcurrentTest {

    class CounterRunnable implements Runnable {

        private UploadStatsProgressListener.FileUploadStat stat;

        public CounterRunnable(UploadStatsProgressListener.FileUploadStat stat) {
            this.stat = stat;
        }

        public void run() {
            for (int i = 0; i < 1000000; i++) {
                stat.incrementUploadedSize(1);
            }
        }
    }

    @Test
    public void incrementUploadSizeInRaceCondition() throws InterruptedException {
        long total = 1000000 * 2;
        UploadStatsProgressListener.FileUploadStat stat = new UploadStatsProgressListener.FileUploadStat(total, 0);

        Thread thread1 = new Thread(new CounterRunnable(stat));
        thread1.setName("add thread");
        thread1.start();

        Thread thread2 = new Thread(new CounterRunnable(stat));
        thread2.setName("add thread2");
        thread2.start();

        thread1.join();
        thread2.join();

        assertEquals(stat.getUploadedSize(), total);
        assertEquals((double) 1, stat.getUploadRatio(), 0.00001);
    }
}
