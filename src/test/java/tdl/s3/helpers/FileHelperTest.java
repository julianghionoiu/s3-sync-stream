package tdl.s3.helpers;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import static net.trajano.commons.testing.UtilityClassTestUtil.assertUtilityClassWellDefined;

import static org.junit.Assert.*;

public class FileHelperTest {
    
    @Test
    public void shouldSatisfyContractForUtilityClass() throws Exception {
        assertUtilityClassWellDefined(FileHelper.class);
    }
 
    @Test
    public void getLockFilePath_returnsCorrect() throws Exception {
        Path path = Paths.get("src", "test", "resources", "test_lock", "file1.txt");
        Path lockFilePath = FileHelper.getLockFilePath(path.toFile());
        
        Path expectedLockFilePath = Paths.get("src", "test", "resources", "test_lock", "file1.txt.lock").toAbsolutePath();
        assertEquals(lockFilePath, expectedLockFilePath);
    }
    
    @Test
    public void lockFileExists_returnsTrue() throws Exception {
        Path path = Paths.get("src", "test", "resources", "test_lock", "file1.txt");
        boolean exists = FileHelper.lockFileExists(path.toFile());
        assertTrue(exists);
    }
    
    @Test
    public void lockFileExists_returnsFalse() throws Exception {
        Path path = Paths.get("src", "test", "resources", "test_lock", "file2.txt");
        boolean exists = FileHelper.lockFileExists(path.toFile());
        assertFalse(exists);
    }
}
