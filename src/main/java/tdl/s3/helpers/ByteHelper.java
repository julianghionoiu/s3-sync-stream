package tdl.s3.helpers;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ByteHelper {

    private static final int MINIMUM_PART_SIZE = 5 * 1024 * 1024;

    public static ByteArrayInputStream createInputStream(byte[] bytes) {
        return new ByteArrayInputStream(bytes, 0, bytes.length);
    }

    public static byte[] truncate(byte[] nextPartBytes, int partSize) {
        if (partSize == nextPartBytes.length) {
            return nextPartBytes;
        }
        byte[] result = new byte[partSize];
        System.arraycopy(nextPartBytes, 0, result, 0, partSize);
        return result;
    }

    public static byte[] getNextPartFromInputStream(InputStream stream, long offset, boolean readLastBytes) throws IOException {
        byte[] buffer = new byte[MINIMUM_PART_SIZE];
        int read = 0;
        skipOffsetInInputStream(stream, offset);
        int available = stream.available();
        if (available < MINIMUM_PART_SIZE && !readLastBytes) {
            return new byte[0];
        }
        while (available > 0) {
            int currentRed = stream.read(buffer, read, MINIMUM_PART_SIZE - read);
            read += currentRed;
            available = stream.available();
            if (read == MINIMUM_PART_SIZE) {
                break;
            }
        }
        return truncate(buffer, read);
    }

    public static void skipOffsetInInputStream(InputStream stream, long offset) throws IOException {
        long skipped = 0;
        while (skipped < offset) {
            skipped += stream.skip(offset);
        }
    }

    public static byte[] readPart(Integer partNumber, File file) {
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            long offset = MINIMUM_PART_SIZE * (partNumber - 1);
            return getNextPartFromInputStream(inputStream, offset, false);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe.getMessage(), ioe);
        }
    }

}
