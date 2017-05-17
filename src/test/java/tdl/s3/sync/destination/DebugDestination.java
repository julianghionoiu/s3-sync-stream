package tdl.s3.sync.destination;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.upload.MultipartUploadResult;

@Slf4j
public class DebugDestination implements Destination {

    private final S3BucketDestination destination;

    private int count = 0;

    public DebugDestination(S3BucketDestination destination) {
        this.destination = destination;
    }

    public int getCount() {
        return count;
    }

    @Override
    public boolean canUpload(String remotePath) {
        log.debug("canUpload: START");
        boolean result =  destination.canUpload(remotePath);
        log.debug("canUpload: FINISH");
        return result;
    }

    @Override
    public String initUploading(String remotePath) {
        log.debug("initUploading: START");
        String result = destination.initUploading(remotePath);
        log.debug("initUploading: FINISH");
        return result;
    }

    @Override
    public PartListing getAlreadyUploadedParts(String remotePath) {
        log.debug("getAlreadyUploadedParts: START");
        PartListing result = destination.getAlreadyUploadedParts(remotePath);
        log.debug("getAlreadyUploadedParts: FINISH");
        return result;
    }

    @Override
    public MultipartUploadResult uploadMultiPart(UploadPartRequest request) {
        log.debug("uploadMultiPart: START");
        MultipartUploadResult result = destination.uploadMultiPart(request);
        log.debug("uploadMultiPart: FINISH");
        return result;
    }

    @Override
    public void commitMultipartUpload(String remotePath, List<PartETag> eTags, String uploadId) {
        log.debug("commitMultipartUpload: START");
        destination.commitMultipartUpload(remotePath, eTags, uploadId);
        log.debug("commitMultipartUpload: FINISH");
    }

    @Override
    public UploadPartRequest createUploadPartRequest(String remotePath) {
        log.debug("createUploadPartRequest: START");
        UploadPartRequest result = destination.createUploadPartRequest(remotePath);
        log.debug("createUploadPartRequest: FINISH");
        return result;
    }

}
