package tdl.s3.sync.destination;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.upload.MultipartUploadResult;

import java.util.List;

@Slf4j
public class PerformanceMeasureDestination implements Destination {

    private final Destination destination;

    private int performanceScore = 0;

    public PerformanceMeasureDestination(Destination destination) {
        this.destination = destination;
    }

    public int getPerformanceScore() {
        return performanceScore;
    }

    @Override
    public boolean canUpload(String remotePath) {
        performanceScore += 1;
        return destination.canUpload(remotePath);
    }

    @Override
    public String initUploading(String remotePath) {
        performanceScore += 1;
        return destination.initUploading(remotePath);
    }

    @Override
    public PartListing getAlreadyUploadedParts(String remotePath) {
        performanceScore += 1;
        return destination.getAlreadyUploadedParts(remotePath);
    }

    @Override
    public MultipartUploadResult uploadMultiPart(UploadPartRequest request) {
        performanceScore += 1000;
        return destination.uploadMultiPart(request);
    }

    @Override
    public void commitMultipartUpload(String remotePath, List<PartETag> eTags, String uploadId) {
        performanceScore += 1;
        destination.commitMultipartUpload(remotePath, eTags, uploadId);
    }

    @Override
    public UploadPartRequest createUploadPartRequest(String remotePath) {
        performanceScore += 0;
        return destination.createUploadPartRequest(remotePath);
    }

}
