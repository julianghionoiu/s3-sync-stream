package tdl.s3.sync.progress;

import java.io.File;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import static org.mockito.Mockito.*;

public class UploadStatsProgressListenerTest {

    private UploadStatsProgressListener listener;
    private File file;

    @Before
    public void setUp() {
        listener = new UploadStatsProgressListener();
        file = mock(File.class);
        when(file.length()).thenReturn(new Long(1000000));
    }

    @Test
    public void isCurrentlyUploadingShouldReturnFalse() {
        assertFalse(listener.isCurrentlyUploading());
    }

    @Test
    public void isCurrentlyUploadingShouldReturnTrue() {
        listener.uploadFileStarted(file, "upload", 0);
        assertTrue(listener.isCurrentlyUploading());
    }

    @Test
    public void handleTimestampZeroFileUploadStat() throws InterruptedException {
        listener.uploadFileStarted(file, "upload", 0);
        UploadStatsProgressListener.FileUploadStat stat = listener.getCurrentStats().get();
        assertEquals(stat.getMBps(), 0.0, 0.1);
        Thread.sleep(100);
        stat.incrementUploadedSize(500000);
        assertNotEquals(stat.getMBps(), 0.0, 0.1);
    }

    @Test
    public void upload() {
        listener.uploadFileStarted(file, "upload", 0);
        UploadStatsProgressListener.FileUploadStat stat = listener.getCurrentStats().get();
        assertEquals(stat.getTotalSize(), 1000000);
        assertEquals(stat.getUploadedSize(), 0);
        listener.uploadFileProgress("upload", 500000);
        assertEquals(stat.getUploadedSize(), 500000);
        assertEquals(stat.getUploadRatio(), 0.5, 0.001);
        listener.uploadFileFinished(file);
        assertFalse(listener.isCurrentlyUploading());
    }
}
