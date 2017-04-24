package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUploadingService {

    private final AmazonS3 amazonS3;
    private final String bucket;

    public FileUploadingService(AmazonS3 amazonS3, String bucket) {
        this.amazonS3 = amazonS3;
        this.bucket = bucket;
    }

    public void upload(File file) {
        upload(file, file.getName());
    }

    public void upload(File file, String name) {
        FileUploader fileUploader = bringFileUploader(name, bucket);
        fileUploader.upload(file, name);
    }

    private FileUploader bringFileUploader(String name, String bucket) {
        MultiPartUploadFileUploadingStrategy uploadingStrategy = getMultipartUploads(amazonS3, bucket).stream()
                .filter(upload -> upload.getKey().equals(name))
                .findAny()
                .map(upload -> new MultiPartUploadFileUploadingStrategy(upload))
                .orElseGet(() -> new MultiPartUploadFileUploadingStrategy(null));

        return new FileUploaderImpl(amazonS3, bucket, uploadingStrategy);
    }

    private List<MultipartUpload> getMultipartUploads(AmazonS3 s3, String bucket) {
        ListMultipartUploadsRequest uploadsRequest = new ListMultipartUploadsRequest(bucket);
        MultipartUploadListing multipartUploadListing = s3.listMultipartUploads(uploadsRequest);

        return Stream.of(multipartUploadListing)
                .flatMap(listing -> getNextListing(s3, listing, bucket))
                .map(MultipartUploadListing::getMultipartUploads)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private Stream<MultipartUploadListing> getNextListing(AmazonS3 s3, MultipartUploadListing listing, String bucket) {
        if (listing.isTruncated()) {
            ListMultipartUploadsRequest uploadsRequest = new ListMultipartUploadsRequest(bucket);
            uploadsRequest.setUploadIdMarker(listing.getNextUploadIdMarker());
            uploadsRequest.setKeyMarker(listing.getNextKeyMarker());
            Stream<MultipartUploadListing> head = Stream.of(listing);
            Stream<MultipartUploadListing> tail = getNextListing(s3, s3.listMultipartUploads(uploadsRequest), bucket);
            return Stream.concat(head, tail);
        } else {
            return Stream.of(listing);
        }
    }
}
