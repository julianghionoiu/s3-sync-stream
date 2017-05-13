package tdl.s3.helpers;

import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import tdl.s3.sync.Destination;

public class ExistingMultipartUploadFinder {

    private final Destination destination;

    public ExistingMultipartUploadFinder(Destination destination) {
        this.destination = destination;
    }

    public List<MultipartUpload> getAlreadyStartedMultipartUploads() {
        ListMultipartUploadsRequest uploadsRequest = createListMultipartUploadsRequest();
        MultipartUploadListing multipartUploadListing = destination.getClient().listMultipartUploads(uploadsRequest);

        Stream<MultipartUploadListing> stream = Stream.of(multipartUploadListing)
                .flatMap(listing -> streamNextListing(listing));

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

        return destination.getClient().listMultipartUploads(uploadsRequest);
    }

    private ListMultipartUploadsRequest createListMultipartUploadsRequest() {
        ListMultipartUploadsRequest uploadsRequest = new ListMultipartUploadsRequest(destination.getBucket());
        uploadsRequest.setPrefix(destination.getPrefix());
        return uploadsRequest;
    }

    public MultipartUpload findOrNull(String remotePath) {
        List<MultipartUpload> uploads = getAlreadyStartedMultipartUploads();
        return uploads.stream()
                .filter(upload -> upload.getKey().equals(destination.getFullPath(remotePath)))
                .findAny()
                .orElse(null);
    }
}
