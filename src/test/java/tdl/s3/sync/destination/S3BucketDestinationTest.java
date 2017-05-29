package tdl.s3.sync.destination;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;

public class S3BucketDestinationTest {

    @Test
    public void canUploadReturnsFalse() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        doThrow(new NotFoundException("")).when(awsClient).getObjectMetadata(anyString(), anyString());
        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        boolean canUpload = destination.canUpload("");
        assertFalse(canUpload);
    }

    @Test
    public void canUploadReturnsFalseWhen404Thrown() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        AmazonS3Exception exception = mock(AmazonS3Exception.class);
        doReturn(404).when(exception).getStatusCode();
        doThrow(exception).when(awsClient).getObjectMetadata(anyString(), anyString());
        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        destination.canUpload("");
        boolean canUpload = destination.canUpload("");
        assertFalse(canUpload);
    }

    @Test(expected = DestinationOperationException.class)
    public void canUploadThrowsDestinationOperationException() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        AmazonS3Exception exception = mock(AmazonS3Exception.class);
        doReturn(400).when(exception).getStatusCode();
        doThrow(exception).when(awsClient).getObjectMetadata(anyString(), anyString());
        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        destination.canUpload("");
    }

    @Test(expected = DestinationOperationException.class)
    public void initUploadingThrowsDestinationOperationException() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        AmazonS3Exception exception = mock(AmazonS3Exception.class);
        doThrow(exception).when(awsClient).initiateMultipartUpload(any());
        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        destination.initUploading("");
    }

    @Test(expected = DestinationOperationException.class)
    public void getAlreadyUploadedPartsThrowsDestinationOperationExceptionWhenListMultipartUploadsThrowsException() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        AmazonS3Exception exception = mock(AmazonS3Exception.class);
        doThrow(exception).when(awsClient).listMultipartUploads(any());
        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        destination.getAlreadyUploadedParts("");
    }

    @Test
    public void getAlreadyUploadedPartsRunsNormalWhenNextListingThrowsException() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        AmazonS3Exception exception = mock(AmazonS3Exception.class);
        MultipartUploadListing listing = mock(MultipartUploadListing.class);
        when(listing.isTruncated()).thenReturn(true);
        when(awsClient.listMultipartUploads(any()))
                .thenReturn(listing)
                .thenThrow(exception);

        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        assertNull(destination.getAlreadyUploadedParts(""));
    }

    @Test
    public void getAlreadyUploadedPartsRunsNormalWhenStreamNextListingThrowsException() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        AmazonS3Exception exception = mock(AmazonS3Exception.class);
        MultipartUploadListing listing = mock(MultipartUploadListing.class);
        when(listing.isTruncated()).thenReturn(false);
        when(awsClient.listMultipartUploads(any()))
                .thenReturn(listing)
                .thenThrow(exception);

        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        assertNull(destination.getAlreadyUploadedParts(""));
    }

    @Test(expected = DestinationOperationException.class)
    public void commitMultipartUploadThrowsDestinationOperationException() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        AmazonS3Exception exception = mock(AmazonS3Exception.class);
        doThrow(exception).when(awsClient).completeMultipartUpload(any());
        List<PartETag> eTags = mock(List.class);
        eTags.add(mock(PartETag.class));
        eTags.add(mock(PartETag.class));
        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        destination.commitMultipartUpload("", eTags, "");
    }

    @Test(expected = DestinationOperationException.class)
    public void uploadMultiPartThrowsDestinationOperationException() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        AmazonS3Exception exception = mock(AmazonS3Exception.class);
        UploadPartRequest request = mock(UploadPartRequest.class);

        doThrow(exception).when(awsClient).uploadPart(any());
        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        destination.uploadMultiPart(request);
    }

    @Test
    public void createUploadPartRequest() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .build();
        Object retval = destination.createUploadPartRequest("");
        assertThat(retval, instanceOf(UploadPartRequest.class));
    }

    @Test
    public void filterUploadableFilesShouldAcceptAllIfS3DirectoryIsEmpty() throws DestinationOperationException {

        ObjectListing listing = mock(ObjectListing.class);
        List<S3ObjectSummary> summaries = new ArrayList<>();
        doReturn(summaries).when(listing).getObjectSummaries();

        AmazonS3 awsClient = mock(AmazonS3.class);
        doReturn(listing).when(awsClient).listObjects(anyString(), anyString());

        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .prefix("prefix/")
                .build();
        List<String> paths = Arrays.asList(
                "file1.txt",
                "file2.txt",
                "file3.txt",
                "file4.txt",
                "file5.txt",
                "file6.txt"
        );
        List<String> result = destination.filterUploadableFiles(paths);
        List<String> expected = Arrays.asList(
                "file1.txt",
                "file2.txt",
                "file3.txt",
                "file4.txt",
                "file5.txt",
                "file6.txt"
        );
        Collections.sort(result);
        Collections.sort(expected);
        assertEquals(result, expected);
    }

    @Test
    public void filterUploadableFilesShouldRemoveFilesExistingInS3Directory() throws DestinationOperationException {

        ObjectListing listing = mock(ObjectListing.class);

        List<String> existingPaths = Arrays.asList(
                "prefix/file1.txt",
                "prefix/subdir/file2.txt",
                "prefix/file6.txt"
        );

        List<S3ObjectSummary> summaries = existingPaths.stream()
                .map(path -> {
                    S3ObjectSummary summary = mock(S3ObjectSummary.class);
                    doReturn(path).when(summary).getKey();
                    return summary;
                }).collect(Collectors.toList());

        doReturn(summaries).when(listing).getObjectSummaries();

        AmazonS3 awsClient = mock(AmazonS3.class);
        doReturn(listing).when(awsClient).listObjects(anyString(), anyString());

        Destination destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .prefix("prefix/")
                .build();
        
        List<String> paths = Arrays.asList(
                "file1.txt",
                "subdir/file2.txt",
                "file3.txt",
                "subdir/file4.txt",
                "file5.txt",
                "file6.txt"
        );
        List<String> result = destination.filterUploadableFiles(paths);
        List<String> expected = Arrays.asList(
                "file3.txt",
                "subdir/file4.txt",
                "file5.txt"
        );
        Collections.sort(result);
        Collections.sort(expected);
        assertEquals(result, expected);
    }
}
