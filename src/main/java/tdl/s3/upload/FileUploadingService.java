package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author vdanyliuk
 * @version 11.04.17.
 */
public class FileUploadingService {

    public static final Integer MULTIPART_UPLOAD_SIZE_LIMIT = 5;
    private static final long BYTES_IN_MEGABYTE = 1024 * 1024;

    private final Map<Integer, ? extends FileUploader> uploaderByFileSize;

    public FileUploadingService(Map<Integer, FileUploader> uploaderByFileSize) {
        this.uploaderByFileSize = uploaderByFileSize;
    }

    public FileUploadingService(AmazonS3 amazonS3, String bucketName) {
        FileUploaderImpl smallFilesUploader = new FileUploaderImpl(amazonS3, bucketName, new LargeFileUploadingStrategy());
        FileUploaderImpl largeFilesUploader = new FileUploaderImpl(amazonS3, bucketName, new SmallFileUploadingStrategy());

        this.uploaderByFileSize = new LinkedHashMap<Integer, FileUploaderImpl>(){{
            put(MULTIPART_UPLOAD_SIZE_LIMIT, largeFilesUploader);
            put(Integer.MAX_VALUE, smallFilesUploader);
        }};
    }

    public void upload(File file) {
        upload(file, file.getName());
    }

    public void upload(File file, String name) {
        int fileSizeInMb = (int) (file.length() / BYTES_IN_MEGABYTE);
        FileUploader fileUploader = uploaderByFileSize.keySet().stream()
                .sorted()
                .filter(limit -> limit > fileSizeInMb)
                .findFirst()
                .map(uploaderByFileSize::get)
                .orElseThrow(() -> new IllegalStateException("No file uploader provided " +
                        "for files with size " + fileSizeInMb + " MB."));
        fileUploader.upload(file, name);
    }
}
