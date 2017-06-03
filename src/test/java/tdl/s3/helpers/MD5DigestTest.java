package tdl.s3.helpers;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.junit.Assert.*;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MD5Digest.class, MessageDigest.class})
public class MD5DigestTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test(expected = RuntimeException.class)
    public void handleNotFoundAlgorithmException() throws Exception {
        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("Can't send multipart upload. Can't create MD5 digest. Algorithm not found");

        mockStatic(MessageDigest.class);
        when(MessageDigest.getInstance("MD5"))
                .thenThrow(new NoSuchAlgorithmException("Algorithm not found"));

        byte[] bytes = "Hello World!".getBytes();
        MD5Digest.digest(bytes);
    }

    @Test
    public void digest() {
        byte[] bytes = "Hello World!".getBytes();
        assertEquals(MD5Digest.digest(bytes), "7Qdih1MuhjZehB6Sv8UNjA==");
    }
}
