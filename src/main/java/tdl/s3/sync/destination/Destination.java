package tdl.s3.sync.destination;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import tdl.s3.upload.MultipartUploadResult;

import java.util.List;

public interface Destination {

    boolean canUpload(String remotePath) throws DestinationOperationException;

    String initUploading(String remotePath) throws DestinationOperationException;

    PartListing getAlreadyUploadedParts(String remotePath) throws DestinationOperationException;

    MultipartUploadResult uploadMultiPart(UploadPartRequest request) throws DestinationOperationException;

    void commitMultipartUpload(String remotePath, List<PartETag> eTags, String uploadId) throws DestinationOperationException;

    UploadPartRequest createUploadPartRequest(String remotePath) throws DestinationOperationException;
}
