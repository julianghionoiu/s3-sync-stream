package tdl.s3.upload;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.helpers.ByteHelper;
import tdl.s3.helpers.ChecksumHelper;
import tdl.s3.helpers.FileHelper;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

@Slf4j
public class MultipartUploadFile {

    //Minimum part size is 5 MB
    private static final int MINIMUM_PART_SIZE = 5 * 1024 * 1024;

    private final File file;

    private final String remotePath;

    private final Destination destination;

    private String uploadId;

    private long uploadedSize = 0;

    private PartListing alreadyUploadedParts;

    private Set<Integer> failedMiddlePartNumbers;

    private int nextPartToUploadIndex = 1;

    private List<PartETag> partETags;

    private boolean isWritingFinished;

    public MultipartUploadFile(File file, String remotePath, Destination destination) throws DestinationOperationException {
        this.file = file;
        this.remotePath = remotePath;
        this.destination = destination;
        init();
    }

    public File getFile() {
        return file;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void incrementUploadedSize(long size) {
        uploadedSize += size;
    }

    public List<PartETag> getPartETags() {
        return partETags;
    }

    public Set<Integer> getFailedMiddlePartNumbers() {
        return failedMiddlePartNumbers;
    }

    private void init() throws DestinationOperationException {
        alreadyUploadedParts = destination.getAlreadyUploadedParts(remotePath);
        isWritingFinished = !FileHelper.lockFileExists(file);
        boolean uploadingStarted = alreadyUploadedParts != null;
        if (!uploadingStarted) {
            uploadId = destination.initUploading(remotePath);
            failedMiddlePartNumbers = Collections.emptySet();
        } else {
            uploadId = alreadyUploadedParts.getUploadId();
            failedMiddlePartNumbers = MultipartUploadHelper.getFailedMiddlePartNumbers(alreadyUploadedParts);
            uploadedSize = MultipartUploadHelper.getUploadedSize(alreadyUploadedParts);
            nextPartToUploadIndex = MultipartUploadHelper.getLastPartIndex(alreadyUploadedParts) + 1;
        }
        partETags = MultipartUploadHelper.getPartETagsFromPartListing(alreadyUploadedParts);
    }

    public void validateUploadedFileSize() {
        if (file.length() < uploadedSize) {
            throw new IllegalStateException(
                    "Already uploaded size of file " + file.getName()
                    + " is greater than actual file size. "
                    + "Probably file was changed and can't be uploaded now."
            );
        }
    }

    public BufferedInputStream createBufferedInputStreamFromFile() throws FileNotFoundException {
        return new BufferedInputStream(new FileInputStream(file));
    }

    public UploadPartRequest createUploadPartRequest() throws DestinationOperationException {
        return destination.createUploadPartRequest(remotePath)
                .withUploadId(uploadId);
    }

    public UploadPartRequest getUploadPartRequestForData(byte[] nextPart, boolean isLastPart, int partNumber) throws IOException, DestinationOperationException {
        try (ByteArrayInputStream partInputStream = ByteHelper.createInputStream(nextPart)) {
            UploadPartRequest request = createUploadPartRequest()
                    .withPartNumber(partNumber)
                    .withMD5Digest(ChecksumHelper.digest(nextPart, "MD5"))
                    .withLastPart(isLastPart)
                    .withPartSize(nextPart.length)
                    .withInputStream(partInputStream);
            return request;
        }
    }

    public void commitIfFinishedWriting() throws DestinationOperationException {
        if (isWritingFinished) {
            destination.commitMultipartUpload(remotePath, partETags, uploadId);
        }
    }

    public Stream<UploadPartRequest> streamUploadPartRequestForFailedParts() {
        return getFailedMiddlePartNumbers()
                .stream()
                .map(partNumber -> {
                    try {
                        byte[] partData = readPart(partNumber);
                        UploadPartRequest request = getUploadPartRequestForData(partData, false, partNumber);
                        incrementUploadedSize(partData.length);
                        return request;
                    } catch (IOException | DestinationOperationException ex) {
                        System.out.println(partNumber);
                        return null;
                    }
                })
                .filter(Objects::nonNull);
    }

    public byte[] readPart(int partNumber) throws IOException {
        return ByteHelper.readPart(partNumber, file);
    }

    public Stream<UploadPartRequest> streamUploadPartRequestForIncompleteParts() throws IOException, DestinationOperationException {
        try (InputStream inputStream = createBufferedInputStreamFromFile()) {
            byte[] nextPart = ByteHelper.getNextPartFromInputStream(inputStream, uploadedSize, isWritingFinished);
            int partSize = nextPart.length;
            List<UploadPartRequest> requests = new ArrayList<>();
            while (partSize > 0) {
                boolean isLastPart = isWritingFinished && partSize < MINIMUM_PART_SIZE;
                UploadPartRequest request = getUploadPartRequestForData(nextPart, isLastPart, nextPartToUploadIndex);
                nextPartToUploadIndex++;
                requests.add(request);
                nextPart = ByteHelper.getNextPartFromInputStream(inputStream, 0, isWritingFinished);
                partSize = nextPart.length;
            }
            return requests.stream();
        }
    }
}
