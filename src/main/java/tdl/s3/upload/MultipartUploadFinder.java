package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.sync.destination.DestinationOperationException;

@Slf4j
public class MultipartUploadFinder {

    private final AmazonS3 awsClient;
    private final String bucket;
    private final String prefix;

    public MultipartUploadFinder(AmazonS3 awsClient, String bucket, String prefix) {
        this.awsClient = awsClient;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public List<MultipartUpload> getAlreadyStartedMultipartUploads() throws DestinationOperationException {
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
}
