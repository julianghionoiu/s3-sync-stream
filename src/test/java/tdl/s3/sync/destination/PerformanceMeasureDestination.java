package tdl.s3.sync.destination;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.upload.MultipartUploadResult;

import java.util.List;
import java.util.Map;

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
    public boolean canUpload(String remotePath) throws DestinationOperationException {
        performanceScore += 1;
        return destination.canUpload(remotePath);
    }

    @Override
    public String initUploading(String remotePath) throws DestinationOperationException {
        performanceScore += 1;
        return destination.initUploading(remotePath);
    }

    @Override
    public PartListing getAlreadyUploadedParts(String remotePath) throws DestinationOperationException {
        performanceScore += 1;
        return destination.getAlreadyUploadedParts(remotePath);
    }

    @Override
    public MultipartUploadResult uploadMultiPart(UploadPartRequest request) throws DestinationOperationException {
        performanceScore += 1000;
        return destination.uploadMultiPart(request);
    }

    @Override
    public void commitMultipartUpload(String remotePath, List<PartETag> eTags, String uploadId) throws DestinationOperationException {
        performanceScore += 1;
        destination.commitMultipartUpload(remotePath, eTags, uploadId);
    }

    @Override
    public UploadPartRequest createUploadPartRequest(String remotePath) throws DestinationOperationException {
        performanceScore += 0;
        return destination.createUploadPartRequest(remotePath);
    }

    @Override
    public List<String> filterUploadableFiles(List<String> relativePaths) throws DestinationOperationException {
        performanceScore += 0;
        return destination.filterUploadableFiles(relativePaths);
    }

}
