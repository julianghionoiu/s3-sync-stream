package tdl.s3.upload;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class MultipartUploadResultTest {

    @Test
    public void test() {
        UploadPartRequest request = mock(UploadPartRequest.class);
        UploadPartResult result = mock(UploadPartResult.class);
        MultipartUploadResult res = new MultipartUploadResult(request, result);
        Assertions.assertEquals(res.getRequest(), request);
        Assertions.assertEquals(res.getResult(), result);
    }   
}
