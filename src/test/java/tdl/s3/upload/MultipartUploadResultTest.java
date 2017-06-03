package tdl.s3.upload;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import static org.mockito.Mockito.mock;

public class MultipartUploadResultTest {

    @Test
    public void test() {
        UploadPartRequest request = mock(UploadPartRequest.class);
        UploadPartResult result = mock(UploadPartResult.class);
        MultipartUploadResult res = new MultipartUploadResult(request, result);
        assertEquals(res.getRequest(), request);
        assertEquals(res.getResult(), result);
    }
}
