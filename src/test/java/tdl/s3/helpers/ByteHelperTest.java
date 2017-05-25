package tdl.s3.helpers;

import java.util.Random;
import org.junit.Test;
import static org.junit.Assert.*;

public class ByteHelperTest {

    @Test
    public void truncateShouldReturnExactBytesIfLengthIsTheSame() {
        int size = 100;
        byte[] bytes = new byte[size];
        (new Random()).nextBytes(bytes);
        byte[] truncated = ByteHelper.truncate(bytes, size);
        assertEquals(truncated, bytes);
    }
    
    @Test
    public void truncateShouldReturnSubsetIfPartSizeIsLarger() {
        throw new UnsupportedOperationException("TODO");
    }
    
    @Test
    public void truncateShouldReturnSomethingIfPartSizeIsSmaller() {
        throw new UnsupportedOperationException("TODO");
    }
    
    @Test
    public void getNextPartFromInputStream() {
        throw new UnsupportedOperationException("TODO");
    }
    
    @Test
    public void skipOffsetInInputStream() {
        throw new UnsupportedOperationException("TODO");
    }
    
    @Test
    public void readPart() {
        throw new UnsupportedOperationException("TODO");
    }
}
