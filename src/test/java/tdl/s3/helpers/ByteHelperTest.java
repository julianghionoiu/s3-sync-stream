package tdl.s3.helpers;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class ByteHelperTest {
    
    private static byte[] createRandomBytes(int size) {
        byte[] bytes = new byte[size];
        (new Random()).nextBytes(bytes);
        return bytes;
    }
    
    @Test
    public void createInputStream() {
        byte[] bytes = createRandomBytes(100);
        Object object = ByteHelper.createInputStream(bytes);
        assertThat(object, instanceOf(ByteArrayInputStream.class));
    }

    @Test
    public void truncateShouldReturnExactBytesIfRequestedLengthIsTheSame() {
        int size = 100;
        byte[] bytes = createRandomBytes(size);
        byte[] truncated = ByteHelper.truncate(bytes, size);
        assertArrayEquals(truncated, bytes);
    }
    
    @Test
    public void truncateShouldReturnExactIfRequestedLengthIsLarger() {
        int size = 100;
        byte[] bytes = createRandomBytes(size);
        byte[] truncated = ByteHelper.truncate(bytes, size + 50);
        assertArrayEquals(truncated, bytes);
    }
    
    @Test
    public void truncateShouldReturnSubsetIfPartSizeIsSmaller() {
        int size = 100;
        byte[] bytes = createRandomBytes(size);
        int requestedSize = size - 50;
        byte[] truncated = ByteHelper.truncate(bytes, requestedSize);
        assertEquals(truncated.length, requestedSize);
    }
    
    @Test
    public void getNextPartFromInputStream() {
        throw new UnsupportedOperationException("TODO");
    }
    
    @Test
    public void skipOffsetInInputStream() {
        byte[] bytes = createRandomBytes(100);
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        try {
            ByteHelper.skipOffsetInInputStream(stream, 10);
        } catch (IOException ex) {
            throw new Error();
        }
    }
    
//    @Test
//    public void skipOffsetInInputStreamShouldHandlePrematureStream() {
//        int size = 100;
//        byte[] bytes = createRandomBytes(size);
//        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
//        try {
//            ByteHelper.skipOffsetInInputStream(stream, size + 1);
//        } catch (IOException ex) {
//            throw new Error();
//        }
//    }
    
    @Test
    public void readPart() throws IOException {
        File largeFile = Paths.get("src/test/resources/helpers/bytehelpertest/largefile.bin").toFile();
        for (int i = 1; i <= 2; i++) {
            byte[] readBytes = ByteHelper.readPart(i, largeFile);
            String path = "src/test/resources/helpers/bytehelpertest/part"  + i + ".bin";
            InputStream stream = new FileInputStream(path);
            byte[] compareByte = IOUtils.toByteArray(stream);
            assertEquals(readBytes.length, compareByte.length);
            assertThat(readBytes, equalTo(compareByte));
        }
        //Last part won't get read
        byte[] readBytes = ByteHelper.readPart(3, largeFile);
        assertEquals(readBytes.length, 0);
    }
}
