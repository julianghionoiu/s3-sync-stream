package tdl.s3.helpers;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class MD5Digest {

    private static MessageDigest md5Digest;

    static {
        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Can't send multipart upload. Can't create MD5 digest. " + e.getMessage(), e);
        }
    }

    public static String digest(byte[] bytes) {
        return Base64.getEncoder().encodeToString(md5Digest.digest(bytes));
    }
}
