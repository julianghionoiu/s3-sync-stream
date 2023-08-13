package tdl.s3.helpers;

import org.apache.commons.io.IOUtils;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.*;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static net.trajano.commons.testing.UtilityClassTestUtil.assertUtilityClassWellDefined;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ByteHelperTest {

    private static byte[] createRandomBytes(int size) {
        byte[] bytes = new byte[size];
        (new Random()).nextBytes(bytes);
        return bytes;
    }

    @Test
    public void shouldSatisfyContractForUtilityClass() throws Exception {
        assertUtilityClassWellDefined(ByteHelper.class);
    }

    @Test
    public void createInputStreamInitialisesOffsetToZero() throws IOException {
        byte[] bytes = createRandomBytes(100);
        ByteArrayInputStream stream = ByteHelper.createInputStream(bytes);
        for (int i = 0; i < 5; i++) {
            MatcherAssert.assertThat(stream.read(), equalTo(Byte.toUnsignedInt(bytes[i])));
        }
    }

    @Test
    public void truncateShouldReturnExactBytesIfRequestedLengthIsTheSame() {
        int size = 100;
        byte[] bytes = createRandomBytes(size);
        byte[] truncated = ByteHelper.truncate(bytes, size);
        Assertions.assertArrayEquals(truncated, bytes);
    }

    @Test
    public void truncateShouldReturnExactIfRequestedLengthIsLarger() {
        int size = 100;
        byte[] bytes = createRandomBytes(size);
        byte[] truncated = ByteHelper.truncate(bytes, size + 50);
        Assertions.assertArrayEquals(truncated, bytes);
    }

    @Test
    public void truncateShouldReturnSubsetIfPartSizeIsSmaller() {
        int size = 100;
        byte[] bytes = createRandomBytes(size);
        int requestedSize = size - 50;
        byte[] truncated = ByteHelper.truncate(bytes, requestedSize);
        Assertions.assertEquals(truncated.length, requestedSize);
    }

    @Test
    public void getNextPartFromInputStream() throws IOException {
        File largeFile = Paths.get("src/test/resources/helpers/bytehelpertest/largefile.bin").toFile();
        InputStream stream = new FileInputStream(largeFile);
        String path = "src/test/resources/helpers/bytehelpertest/part2.bin";
        byte[] compareBytes = IOUtils.toByteArray(new FileInputStream(path));
        byte[] readBytes = ByteHelper.getNextPartFromInputStream(stream, 5242880, true);
        MatcherAssert.assertThat(readBytes, equalTo(compareBytes));
    }

    @Test
    public void getNextPartFromInputStreamShouldReadLastByte() throws IOException {
        File largeFile = Paths.get("src/test/resources/helpers/bytehelpertest/largefile.bin").toFile();
        InputStream stream = new FileInputStream(largeFile);
        String path = "src/test/resources/helpers/bytehelpertest/part3.bin";
        byte[] compareBytes = IOUtils.toByteArray(new FileInputStream(path));
        long remainingLength = largeFile.length() - 10485760;
        byte[] readBytes = ByteHelper.getNextPartFromInputStream(stream, 10485760, true);
        Assertions.assertEquals(readBytes.length, remainingLength);
        MatcherAssert.assertThat(readBytes, equalTo(compareBytes));
    }

    @Test
    public void skipOffsetInInputStreamSkipsBytes() throws IOException {
        byte[] bytes = createRandomBytes(100);
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        ByteHelper.skipOffsetInInputStream(stream, 10);
        MatcherAssert.assertThat(stream.read(), equalTo(Byte.toUnsignedInt(bytes[10])));
    }

    //TODO Make this test pass
    @Test
    @Timeout(value = 100, unit = TimeUnit.MILLISECONDS)
    public void skipOffsetInInputStreamThrowsIOExceptionIfNotEnoughBytesWhereSkipped() {
        Assertions.assertThrows(IOException.class, () -> {
            byte[] bytes = createRandomBytes(2);
            ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
            ByteHelper.skipOffsetInInputStream(stream, 10);
        });
    }

    @Test
    public void skipOffsetInInputStreamThrowsIOExceptionIfSkippedLongerThanOffset() {
        Assertions.assertThrows(IOException.class, () -> {
            InputStream stream = mock(InputStream.class);
            doReturn((long) 11).when(stream).skip((long) 10);
            ByteHelper.skipOffsetInInputStream(stream, 10);
        });
    }

    @Test
    public void readPartLoadsBytesFromFile() throws IOException {
        File largeFile = Paths.get("src/test/resources/helpers/bytehelpertest/largefile.bin").toFile();
        for (int i = 1; i <= 2; i++) {
            byte[] readBytes = ByteHelper.readPart(i, largeFile);
            String path = "src/test/resources/helpers/bytehelpertest/part" + i + ".bin";
            InputStream stream = new FileInputStream(path);
            byte[] compareBytes = IOUtils.toByteArray(stream);
            Assertions.assertEquals(readBytes.length, compareBytes.length);
            MatcherAssert.assertThat(readBytes, equalTo(compareBytes));
        }
        //Last part won't get read
        byte[] readBytes = ByteHelper.readPart(3, largeFile);
        Assertions.assertEquals(readBytes.length, 0);
    }
}
