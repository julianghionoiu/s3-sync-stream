package tdl.s3.sync.progress;

import java.io.File;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class UploadStatsProgressListenerTest {

    @Test
    public void isCurrentlyUploadingShouldReturnFalse() {
        UploadStatsProgressListener listener = new UploadStatsProgressListener();
        assertFalse(listener.isCurrentlyUploading());
    }

    @Test
    public void isCurrentlyUploadingShouldReturnTrue() {
        UploadStatsProgressListener listener = new UploadStatsProgressListener();
        File file = mock(File.class);
        when(file.length()).thenReturn(new Long(10));
        listener.uploadFileStarted(file, "upload");
        assertTrue(listener.isCurrentlyUploading());
    }

    @Test
    public void handleTimestampZeroFileUploadStat() {
        UploadStatsProgressListener listener = new UploadStatsProgressListener();
        File file = mock(File.class);
        when(file.length()).thenReturn(new Long(10));
        listener.uploadFileStarted(file, "upload");
        UploadStatsProgressListener.FileUploadStat stat = listener.getCurrentStats().get();
        assertEquals(stat.getMBps(), 0.0, 0.1);
    }
}
