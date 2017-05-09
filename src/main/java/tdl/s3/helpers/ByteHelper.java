package tdl.s3.helpers;

import java.io.ByteArrayInputStream;

public class ByteHelper {

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
}
