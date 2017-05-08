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
import tdl.s3.sync.SyncProgressListener;

public class FileUploadingService {

    private final AmazonS3 amazonS3;

    private final String bucket;

    private final String prefix;

    private List<SyncProgressListener> listeners;

    public FileUploadingService(AmazonS3 amazonS3, String bucket, String prefix) {
        this.amazonS3 = amazonS3;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public void upload(File file) {
        RemoteFile remoteFile = new RemoteFile(bucket, prefix, file.getName(), amazonS3);
        upload(file, remoteFile);
    }

    public void upload(File file, String remoteName) {
        RemoteFile remoteFile = new RemoteFile(bucket, prefix, remoteName, amazonS3);
        upload(file, remoteFile);
    }

    private void upload(File file, RemoteFile remoteFile) {
        FileUploader fileUploader = bringFileUploader(remoteFile);
        fileUploader.upload(file, remoteFile);
    }

    private FileUploader bringFileUploader(RemoteFile remoteFile) {
        List<MultipartUpload> alreadyStartedUploads = getMultipartUploads(amazonS3, remoteFile.getBucket(), remoteFile.getPrefix());
        MultipartUpload multipartUpload = alreadyStartedUploads.stream()
                .filter(upload -> upload.getKey().equals(remoteFile.getFullPath()))
                .findAny()
                .orElse(null);
        UploadingStrategy strategy = new MultiPartUploadFileUploadingStrategy(multipartUpload);
        return new FileUploaderImpl(amazonS3, bucket, prefix, strategy);
    }

    private static List<MultipartUpload> getMultipartUploads(AmazonS3 s3, String bucket, String prefix) {
        ListMultipartUploadsRequest uploadsRequest = new ListMultipartUploadsRequest(bucket);
        uploadsRequest.setPrefix(prefix);
        MultipartUploadListing multipartUploadListing = s3.listMultipartUploads(uploadsRequest);

        return Stream.of(multipartUploadListing)
                .flatMap(listing -> getNextListing(s3, listing, bucket, prefix))
                .map(MultipartUploadListing::getMultipartUploads)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private static Stream<MultipartUploadListing> getNextListing(AmazonS3 s3, MultipartUploadListing listing,
            String bucket, String prefix) {
        if (listing.isTruncated()) {
            ListMultipartUploadsRequest uploadsRequest = new ListMultipartUploadsRequest(bucket);
            uploadsRequest.setPrefix(prefix);
            uploadsRequest.setUploadIdMarker(listing.getNextUploadIdMarker());
            uploadsRequest.setKeyMarker(listing.getNextKeyMarker());
            Stream<MultipartUploadListing> head = Stream.of(listing);
            Stream<MultipartUploadListing> tail = getNextListing(s3, s3.listMultipartUploads(uploadsRequest), bucket, prefix);
            return Stream.concat(head, tail);
        } else {
            return Stream.of(listing);
        }
    }
}
