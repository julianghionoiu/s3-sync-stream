package tdl.s3.upload;

import org.junit.Test;
import static net.trajano.commons.testing.UtilityClassTestUtil.assertUtilityClassWellDefined;

public class MultipartUploadHelperTest {

    @Test
    public void shouldSatisfyContractForUtilityClass() throws Exception {
        assertUtilityClassWellDefined(MultipartUploadHelper.class);
    }
}
