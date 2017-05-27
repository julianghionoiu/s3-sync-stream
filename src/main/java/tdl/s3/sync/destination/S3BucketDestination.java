package tdl.s3.sync.destination;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import tdl.s3.upload.MultipartUploadResult;

@Builder
public class S3BucketDestination implements Destination {
    private final AmazonS3 awsClient;
    private final String bucket;
    private final String prefix;

    // ~~~~ Public methods


    @Override
    public boolean canUpload(String remotePath) throws DestinationOperationException {
        try {
            awsClient.getObjectMetadata(bucket, getFullPath(remotePath));
            return true;
        } catch (NotFoundException ex) {
            return false;
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return false;
            } else {
                throw ex;
            }
        }
    }

    @Override
    public String initUploading(String remotePath) throws DestinationOperationException {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, getFullPath(remotePath));
        InitiateMultipartUploadResult result = awsClient.initiateMultipartUpload(request);
        return result.getUploadId();
    }

    @Override
    public PartListing getAlreadyUploadedParts(String remotePath) throws DestinationOperationException {
        MultipartUpload multipartUpload = findOrNull(remotePath);

        return Optional.ofNullable(multipartUpload)
                .map(MultipartUpload::getUploadId)
                .map(id -> getPartListing(remotePath, id))
                .orElse(null);
    }

    @Override
    public MultipartUploadResult uploadMultiPart(UploadPartRequest request) throws DestinationOperationException {
        UploadPartResult result = awsClient.uploadPart(request);
        return new MultipartUploadResult(request, result);
    }

    @Override
    public void commitMultipartUpload(String remotePath, List<PartETag> eTags, String uploadId) throws DestinationOperationException {
        eTags.sort(Comparator.comparing(PartETag::getPartNumber));
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(
                bucket,
                getFullPath(remotePath),
                uploadId,
                eTags
        );
        completeMultipartUpload(request);
    }

    @Override
    public UploadPartRequest createUploadPartRequest(String remotePath) throws DestinationOperationException {
        return new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(getFullPath(remotePath));
    }

    // ~~~ MultiPart Helpers


    private void completeMultipartUpload(CompleteMultipartUploadRequest request) {
        awsClient.completeMultipartUpload(request);
    }

    private ListMultipartUploadsRequest createListMultipartUploadsRequest() {
        ListMultipartUploadsRequest uploadsRequest = new ListMultipartUploadsRequest(bucket);
        uploadsRequest.setPrefix(prefix);
        return uploadsRequest;
    }

    private MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) {
        return awsClient.listMultipartUploads(request);
    }


    private List<MultipartUpload> getAlreadyStartedMultipartUploads() {
        ListMultipartUploadsRequest uploadsRequest = createListMultipartUploadsRequest();
        MultipartUploadListing multipartUploadListing = listMultipartUploads(uploadsRequest);

        Stream<MultipartUploadListing> stream = Stream.of(multipartUploadListing)
                .flatMap(this::streamNextListing);

        return stream.map(MultipartUploadListing::getMultipartUploads)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private Stream<MultipartUploadListing> streamNextListing(MultipartUploadListing listing) {
        if (!listing.isTruncated()) {
            return Stream.of(listing);
        }
        MultipartUploadListing nextListing = getNextListing(listing);

        Stream<MultipartUploadListing> head = Stream.of(listing);
        Stream<MultipartUploadListing> tail = streamNextListing(nextListing);

        return Stream.concat(head, tail);
    }

    private MultipartUploadListing getNextListing(MultipartUploadListing listing) {
        ListMultipartUploadsRequest uploadsRequest = createListMultipartUploadsRequest();
        uploadsRequest.setUploadIdMarker(listing.getNextUploadIdMarker());
        uploadsRequest.setKeyMarker(listing.getNextKeyMarker());

        return listMultipartUploads(uploadsRequest);
    }

    private MultipartUpload findOrNull(String remotePath) {
        List<MultipartUpload> uploads = getAlreadyStartedMultipartUploads();
        return uploads.stream()
                .filter(upload -> upload.getKey().equals(getFullPath(remotePath)))
                .findAny()
                .orElse(null);
    }

    // ~~~ Part Helpers

    private PartListing getPartListing(String remotePath, String uploadId) {
        ListPartsRequest request = new ListPartsRequest(bucket, getFullPath(remotePath), uploadId);
        return listParts(request);
    }

    private PartListing listParts(ListPartsRequest request) {
        return awsClient.listParts(request);
    }

    // ~~~ Path helpers

    private String getFullPath(String path) {
        return prefix + path;
    }
}
