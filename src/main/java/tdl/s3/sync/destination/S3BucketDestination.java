package tdl.s3.sync.destination;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.upload.MultipartUploadResult;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Builder
@Slf4j
public class S3BucketDestination implements Destination {

    private final AmazonS3 awsClient;
    private final String bucket;
    private final String prefix;

    // ~~~~ Public methods

    @Override
    public void testUploadPermissions() throws DestinationOperationException {
        try{
            // Upload a file to S3 to prove that the user if not expired and has write permissions to the bucket + prefix
            awsClient.putObject(bucket, prefix + "last_sync_start.txt", "timestamp: "+System.currentTimeMillis());
        } catch (AmazonS3Exception ex) {
            throw new DestinationOperationException(ex.getMessage(), ex);
        }
    }

    @Override
    public List<String> filterUploadableFiles(List<String> paths) throws DestinationOperationException {
        Set<String> existingItems = listAllObjects().stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toSet());
        
        int trimLength = prefix.length();
        return paths.stream()
                .map(path -> prefix + path)
                .filter(path -> !existingItems.contains(path))
                .map(path -> path.substring(trimLength))
                .collect(Collectors.toList());
    }

    private Set<S3ObjectSummary> listAllObjects() {
        ListObjectsRequest request = new ListObjectsRequest()
                .withBucketName(bucket)
                .withPrefix(prefix);
        ObjectListing result;
        Set<S3ObjectSummary> summaries = new HashSet<>();
        do {
            result = awsClient.listObjects(request);
            request.setMarker(result.getNextMarker());
            summaries.addAll(result.getObjectSummaries());
        } while (result.isTruncated());
        return summaries;
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
