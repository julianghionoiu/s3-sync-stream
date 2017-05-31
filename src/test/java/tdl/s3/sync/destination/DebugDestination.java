package tdl.s3.sync.destination;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.upload.MultipartUploadResult;

@Slf4j
public class DebugDestination implements Destination {

    private final Destination destination;

    public DebugDestination(Destination destination) {
        this.destination = destination;
    }

    @Override
    public void testUploadPermissions() throws DestinationOperationException {
        log.debug("testUploadPermissions: START");
        destination.testUploadPermissions();
        log.debug("testUploadPermissions: FINISH");
    }

    @Override
    public String initUploading(String remotePath) throws DestinationOperationException {
        log.debug("initUploading: START");
        String result = destination.initUploading(remotePath);
        log.debug("initUploading: FINISH");
        return result;
    }

    @Override
    public PartListing getAlreadyUploadedParts(String remotePath) throws DestinationOperationException {
        log.debug("getAlreadyUploadedParts: START");
        PartListing result = destination.getAlreadyUploadedParts(remotePath);
        log.debug("getAlreadyUploadedParts: FINISH");
        return result;
    }

    @Override
    public MultipartUploadResult uploadMultiPart(UploadPartRequest request) throws DestinationOperationException {
        log.debug("uploadMultiPart: START");
        MultipartUploadResult result = destination.uploadMultiPart(request);
        log.debug("uploadMultiPart: FINISH");
        return result;
    }

    @Override
    public void commitMultipartUpload(String remotePath, List<PartETag> eTags, String uploadId) throws DestinationOperationException {
        log.debug("commitMultipartUpload: START");
        destination.commitMultipartUpload(remotePath, eTags, uploadId);
        log.debug("commitMultipartUpload: FINISH");
    }

    @Override
    public UploadPartRequest createUploadPartRequest(String remotePath) throws DestinationOperationException {
        log.debug("createUploadPartRequest: START");
        UploadPartRequest r = destination.createUploadPartRequest(remotePath);
        log.debug("createUploadPartRequest: FINISH");
        return r;
    }

    @Override
    public List<String> filterUploadableFiles(List<String> relativePaths) throws DestinationOperationException {
        log.debug("canUploadFiles: START");
        List<String> r = destination.filterUploadableFiles(relativePaths);
        log.debug("canUploadFiles: FINISH");
        return r;
    }

}
