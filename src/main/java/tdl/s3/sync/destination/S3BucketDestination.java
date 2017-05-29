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
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.upload.MultipartUploadResult;

@Builder
@Slf4j
public class S3BucketDestination implements Destination {

    private final AmazonS3 awsClient;
    private final String bucket;
    private final String prefix;

    // ~~~~ Public methods
    @Override
    public boolean canUpload(String remotePath) throws DestinationOperationException {
        String path = getFullPath(remotePath);
        try {
            awsClient.getObjectMetadata(bucket, path);
            return true;
        } catch (NotFoundException ex) {
            return false;
        } catch (AmazonS3Exception ex) {
            if (ex.getStatusCode() == 404) {
                return false;
            } else {
                throw new DestinationOperationException("Fail to check remote path: " + path, ex);
            }
        }
    }

    @Override
    public List<String> filterUploadableFiles(List<String> paths) throws DestinationOperationException {
        //TODO: handle a lot of files
        ObjectListing listing = awsClient.listObjects(bucket, prefix);
        Set<String> existingItems = listing.getObjectSummaries().stream()
                .map(summary -> summary.getKey())
                .collect(Collectors.toSet());
        int trimLength = prefix.length();
        return paths.stream()
                .map(path -> prefix + path)
                .filter(path -> !existingItems.contains(path))
                .map(path -> path.substring(trimLength))
                .collect(Collectors.toList());
    }

    @Override
    public String initUploading(String remotePath) throws DestinationOperationException {
        String path = getFullPath(remotePath);
        try {
            InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, path);
            InitiateMultipartUploadResult result = awsClient.initiateMultipartUpload(request);
            return result.getUploadId();
        } catch (AmazonS3Exception ex) {
            throw new DestinationOperationException("Fail to initialize uploading process: " + path, ex);
        }
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
        try {
            UploadPartResult result = awsClient.uploadPart(request);
            return new MultipartUploadResult(request, result);
        } catch (AmazonS3Exception ex) {
            throw new DestinationOperationException("Fail to upload multipart: " + request.getKey() + " #" + request.getPartNumber(), ex);
        }
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
    private void completeMultipartUpload(CompleteMultipartUploadRequest request) throws DestinationOperationException {
        try {
            awsClient.completeMultipartUpload(request);
        } catch (AmazonS3Exception ex) {
            throw new DestinationOperationException("Failed to complete multipart request: " + request.getKey(), ex);
        }
    }

    private ListMultipartUploadsRequest createListMultipartUploadsRequest() {
        ListMultipartUploadsRequest uploadsRequest = new ListMultipartUploadsRequest(bucket);
        uploadsRequest.setPrefix(prefix);
        return uploadsRequest;
    }

    private MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws DestinationOperationException {
        try {
            return awsClient.listMultipartUploads(request);
        } catch (AmazonS3Exception ex) {
            throw new DestinationOperationException("Failed to list upload request: " + request.getBucketName() + "/" + request.getPrefix(), ex);
        }
    }

    private List<MultipartUpload> getAlreadyStartedMultipartUploads() throws DestinationOperationException {
        ListMultipartUploadsRequest uploadsRequest = createListMultipartUploadsRequest();
        MultipartUploadListing multipartUploadListing = listMultipartUploads(uploadsRequest);

        Stream<MultipartUploadListing> stream = Stream.of(multipartUploadListing)
                .flatMap(listing -> {
                    try {
                        return this.streamNextListing(listing);
                    } catch (DestinationOperationException ex) {
                        log.error("Failed to stream next listing " + listing.getUploadIdMarker(), ex);
                        return null;
                    }
                }).filter(Objects::nonNull);

        return stream.map(MultipartUploadListing::getMultipartUploads)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private Stream<MultipartUploadListing> streamNextListing(MultipartUploadListing listing) throws DestinationOperationException {
        if (!listing.isTruncated()) {
            return Stream.of(listing);
        }
        MultipartUploadListing nextListing = getNextListing(listing);

        Stream<MultipartUploadListing> head = Stream.of(listing);
        Stream<MultipartUploadListing> tail = streamNextListing(nextListing);

        return Stream.concat(head, tail);
    }

    private MultipartUploadListing getNextListing(MultipartUploadListing listing) throws DestinationOperationException {
        ListMultipartUploadsRequest uploadsRequest = createListMultipartUploadsRequest();
        uploadsRequest.setUploadIdMarker(listing.getNextUploadIdMarker());
        uploadsRequest.setKeyMarker(listing.getNextKeyMarker());
        return listMultipartUploads(uploadsRequest);
    }

    private MultipartUpload findOrNull(String remotePath) throws DestinationOperationException {
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
