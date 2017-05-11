package tdl.s3.upload;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public class MultipartUploadResult {

    private final UploadPartRequest request;

    private final UploadPartResult result;

    public MultipartUploadResult(UploadPartRequest request, UploadPartResult result) {
        this.request = request;
        this.result = result;
    }

    public UploadPartRequest getRequest() {
        return request;
    }

    public UploadPartResult getResult() {
        return result;
    }

}
