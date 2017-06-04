package tdl.s3.helpers;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public final class ChecksumHelper {

    private ChecksumHelper() {
    }

    public static String digest(byte[] bytes, String algorithm) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
            byte[] digest = messageDigest.digest(bytes);
            return Base64.getEncoder().encodeToString(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("Can't send multipart upload. Can't create " + algorithm + " digest. " + ex.getMessage(), ex);
        }
    }
}
