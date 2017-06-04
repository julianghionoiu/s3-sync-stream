package tdl.s3.helpers;

import static net.trajano.commons.testing.UtilityClassTestUtil.assertUtilityClassWellDefined;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.junit.Assert.*;

public class ChecksumHelperTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void shouldSatisfyContractForUtilityClass() throws Exception {
        assertUtilityClassWellDefined(ChecksumHelper.class);
    }

    @Test
    public void handleNotFoundAlgorithmException() throws Exception {
        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("Can't send multipart upload.");

        byte[] bytes = "Hello World!".getBytes();
        ChecksumHelper.digest(bytes, "SOMETHING");
    }

    @Test
    public void digest() {
        byte[] bytes = "Hello World!".getBytes();
        assertEquals(ChecksumHelper.digest(bytes, "MD5"), "7Qdih1MuhjZehB6Sv8UNjA==");
    }
}
