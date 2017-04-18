package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUploadingService {

    public static final Integer MULTIPART_UPLOAD_SIZE_LIMIT = 5;
    private static final long BYTES_IN_MEGABYTE = 1024 * 1024;

    private final Map<Integer, ? extends UploadingStrategy> uploaderByFileSize;
    private final AmazonS3 amazonS3;
    private final String bucket;
    private final FileUploader fileUploader;

    public FileUploadingService(AmazonS3 amazonS3, String bucket, FileUploader fileUploader) {
        this.amazonS3 = amazonS3;
        this.bucket = bucket;
        this.fileUploader = fileUploader;

        this.uploaderByFileSize = new LinkedHashMap<Integer, UploadingStrategy>(){{
            put(MULTIPART_UPLOAD_SIZE_LIMIT, new SmallFileUploadingStrategy());
            put(Integer.MAX_VALUE, new LargeFileUploadingStrategy());
        }};
    }

    public void upload(File file) {
        upload(file, file.getName());
    }

    public void upload(File file, String name) {
        FileUploader fileUploader = bringFileUploader(file, name, bucket);
        fileUploader.upload(file, name);
    }

    private FileUploader bringFileUploader(File file, String name, String bucket) {
        List<MultipartUpload> alreadyStartedUploads = getMultipartUploads(amazonS3, bucket);
        MultipartUpload multipartUpload = alreadyStartedUploads.stream()
                .filter(upload -> upload.getKey().equals(name))
                .findAny()
                .orElse(null);

        if (multipartUpload != null) {
            UnfinishedUploadingFileUploadingStrategy uploadingStrategy = new UnfinishedUploadingFileUploadingStrategy(multipartUpload);
            fileUploader.setStrategy(uploadingStrategy);
            return fileUploader;
        } else if (Files.exists(file.toPath().getParent().resolve(file.getName() + ".lock"))) {
            UnfinishedUploadingFileUploadingStrategy uploadingStrategy = new UnfinishedUploadingFileUploadingStrategy(null);
            fileUploader.setStrategy(uploadingStrategy);
            return fileUploader;
        } else {
            return getUploaderByFileSize(file);
        }
    }

    private FileUploader getUploaderByFileSize(File file) {
        int fileSizeInMb = (int) (file.length() / BYTES_IN_MEGABYTE);
        UploadingStrategy strategy = uploaderByFileSize.keySet().stream()
                .sorted()
                .filter(limit -> limit > fileSizeInMb)
                .findFirst()
                .map(uploaderByFileSize::get)
                .orElseThrow(() -> new IllegalStateException("No file uploader provided " +
                        "for files with size " + fileSizeInMb + " MB."));
        fileUploader.setStrategy(strategy);
        return fileUploader;
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
