package tdl.s3.helpers;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class MD5Digest {

    private static MessageDigest md5Digest;

    public static String digest(byte[] bytes) {
        if (md5Digest == null) {
            try {
                md5Digest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Can't send multipart upload. Can't create MD5 digest. " + e.getMessage(), e);
            }
        }
        byte[] digest = md5Digest.digest(bytes);
        return Base64.getEncoder().encodeToString(digest);
    }
}
