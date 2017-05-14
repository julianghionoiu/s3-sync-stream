package tdl.s3.sync.destination;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import tdl.s3.upload.MultipartUploadResult;

import java.util.List;

public interface Destination {
    boolean canUpload(String remotePath);

    String initUploading(String remotePath);

    PartListing getAlreadyUploadedParts(String remotePath);

    MultipartUploadResult uploadMultiPart(UploadPartRequest request);

    void commitMultipartUpload(String remotePath, List<PartETag> eTags, String uploadId);

    UploadPartRequest createUploadPartRequest(String remotePath);
}
