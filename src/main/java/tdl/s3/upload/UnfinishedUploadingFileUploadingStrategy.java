package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class UnfinishedUploadingFileUploadingStrategy implements UploadingStrategy {

    //Minimum part size is 5 MB
    private static final int MINIMUM_PART_SIZE = 5 * 1024 * 1024;
    private final MultipartUpload upload;

    private MessageDigest md5Digest;

    private String uploadId;

    private List<PartETag> tags;

    private File file;

    private long uploadedSize;

    private int nextPartToUploadIndex;

    private boolean writingFinished;

    public UnfinishedUploadingFileUploadingStrategy(MultipartUpload upload) {
        this.upload = upload;
        try {
            md5Digest = MessageDigest.getInstance("MD5");
            tags = new ArrayList<>();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Can't send multipart upload. Can't create MD5 digest. " + e.getMessage(), e);
        }
    }

    @Override
    public void upload(AmazonS3 s3, String bucket, File file, String newName) throws Exception {
        initStrategy(s3, bucket, file, newName, upload);
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            uploadRequiredParts(s3, bucket, newName, inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Error while reading file " + file + ". " + e.getMessage(), e);
        }

    }

    private void initStrategy(AmazonS3 s3, String bucket, File file, String newName, MultipartUpload upload) {
        writingFinished = ! Files.exists(getLockFilePath(file));
        this.file = file;
        PartListing alreadyUploadedParts = getAlreadyUploadedParts(s3, bucket, newName, upload);
        boolean uploadingStarted = alreadyUploadedParts != null;
        if (!uploadingStarted) {
            uploadId = initUploading(s3, bucket, newName);
            uploadedSize = 0;
            nextPartToUploadIndex = 0;
        } else {
            uploadId = alreadyUploadedParts.getUploadId();
            uploadedSize = getUploadedSize(alreadyUploadedParts);
            nextPartToUploadIndex = getLastPartIndex(alreadyUploadedParts);
        }
        try {
            if (Files.size(file.toPath()) < uploadedSize) {
                throw new IllegalStateException("Already uploaded size of file " + file + " is greater than actual file size. " +
                        "Probably file was changed and can't be uploaded now.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Can't read size of file to upload, " + file + ". " + e.getMessage(), e);
        }
    }

    private Path getLockFilePath(File file) {
        return file.toPath()
                .toAbsolutePath()
                .normalize()
                .getParent()
                .resolve(file.getName() + ".lock");
    }

    private void uploadRequiredParts(AmazonS3 s3, String bucket, String newName, InputStream inputStream) throws IOException {
        if (writingFinished) {
            uploadAndCommit(s3, bucket, newName);
        } else {
            uploadIncompleteParts(s3, bucket, newName, inputStream);
        }
    }

    private void uploadIncompleteParts(AmazonS3 s3, String bucket, String newName, InputStream inputStream) throws IOException {
        byte[] nextPartBytes = new byte[MINIMUM_PART_SIZE * 2];

        for (int partSize = getNextPart(nextPartBytes, uploadedSize, inputStream);
             partSize > 0;
             partSize = getNextPart(nextPartBytes, 0, inputStream))

        {
            InputStream partInputStream = getInputStream(nextPartBytes, partSize);
            String digest = getDigest(nextPartBytes);
            UploadPartRequest request = getUploadPartRequest(bucket, newName, nextPartToUploadIndex++, partInputStream, partSize, digest);
            UploadPartResult result = s3.uploadPart(request);
            tags.add(result.getPartETag());
        }
    }

    private void uploadAndCommit(AmazonS3 s3, String bucket, String newName) {
        UploadPartRequest partRequest = new UploadPartRequest()
                .withFileOffset(uploadedSize)
                .withFile(file)
                .withPartNumber(nextPartToUploadIndex)
                .withBucketName(bucket)
                .withKey(newName);

        UploadPartResult result = s3.uploadPart(partRequest);
        tags.add(result.getPartETag());

        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(bucket, newName, uploadId, tags);
        s3.completeMultipartUpload(request);
    }

    private UploadPartRequest getUploadPartRequest(String bucket,
                                                   String newName,
                                                   int partNumber,
                                                   InputStream partInputStream,
                                                   int partSize,
                                                   String digest) {
        return new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(newName)
                .withMD5Digest(digest)
                .withPartNumber(partNumber)
                .withPartSize(partSize)
                .withUploadId(uploadId)
                .withInputStream(partInputStream);
    }

    private InputStream getInputStream(byte[] nextPartBytes, int partSize) {
        return new ByteArrayInputStream(nextPartBytes, 0, partSize);
    }

    private String getDigest(byte[] nextPartBytes) {
        return Hex.encodeHexString(md5Digest.digest(nextPartBytes));
    }

    private int getNextPart(byte[] nextPartBytes, long offset, InputStream inputStream) throws IOException {
        int read = 0;
        skipAlreadyUploadedParts(offset, inputStream);
        while (inputStream.available() > 0) {
            int currentRed = inputStream.read(nextPartBytes, read, MINIMUM_PART_SIZE - read);
            read += currentRed;
            if (read == MINIMUM_PART_SIZE) break;
        }
        return read;
    }

    private void skipAlreadyUploadedParts(long offset, InputStream inputStream) throws IOException {
        long skipped = 0;
        while (skipped < offset) {
            skipped += inputStream.skip(offset);
        }
    }

    private String initUploading(AmazonS3 s3, String bucket, String newName) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, newName);
        InitiateMultipartUploadResult result = s3.initiateMultipartUpload(request);
        return result.getUploadId();
    }

    private long getUploadedSize(PartListing partListing) {
        return partListing.getParts().stream()
                .mapToLong(PartSummary::getSize)
                .sum();
    }

    private int getLastPartIndex(PartListing partListing) {
        return partListing.getParts().stream()
                .mapToInt(PartSummary::getPartNumber)
                .max()
                .orElse(0);
    }

    private PartListing getAlreadyUploadedParts(AmazonS3 s3, String bucket, String key, MultipartUpload upload) {
        return Optional.ofNullable(upload)
                .map(MultipartUpload::getUploadId)
                .map(id -> getPartListing(s3, bucket, key, id))
                .orElse(null);
    }

    private PartListing getPartListing(AmazonS3 s3, String bucket, String key, String uploadId) {
        ListPartsRequest request = new ListPartsRequest(bucket, key, uploadId);
        return s3.listParts(request);
    }

}
