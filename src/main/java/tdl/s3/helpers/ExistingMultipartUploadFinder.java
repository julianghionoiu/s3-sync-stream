package tdl.s3.helpers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import tdl.s3.upload.RemoteFile;

public class ExistingMultipartUploadFinder {

    private final AmazonS3 client;

    private final String bucket;

    private final String prefix;

    public ExistingMultipartUploadFinder(AmazonS3 client, String bucket, String prefix) {
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public List<MultipartUpload> getAlreadyStartedMultipartUploads() {
        ListMultipartUploadsRequest uploadsRequest = createListMultipartUploadsRequest();
        MultipartUploadListing multipartUploadListing = client.listMultipartUploads(uploadsRequest);

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
        
        return client.listMultipartUploads(uploadsRequest);
    }

    private ListMultipartUploadsRequest createListMultipartUploadsRequest() {
        ListMultipartUploadsRequest uploadsRequest = new ListMultipartUploadsRequest(bucket);
        uploadsRequest.setPrefix(prefix);
        return uploadsRequest;
    }

    public MultipartUpload findOrNull(RemoteFile remoteFile) {
        List<MultipartUpload> uploads = getAlreadyStartedMultipartUploads();
        return uploads.stream()
                .filter(upload -> upload.getKey().equals(remoteFile.getFullPath()))
                .findAny()
                .orElse(null);
    }
}
