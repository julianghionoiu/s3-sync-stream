package tdl.s3.upload;

import org.junit.jupiter.api.Test;

import static net.trajano.commons.testing.UtilityClassTestUtil.assertUtilityClassWellDefined;

public class MultipartUploadHelperTest {

    @Test
    public void shouldSatisfyContractForUtilityClass() {
        assertUtilityClassWellDefined(MultipartUploadHelper.class);
    }
}
