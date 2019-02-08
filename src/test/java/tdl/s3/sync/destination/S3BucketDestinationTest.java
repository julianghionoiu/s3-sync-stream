package tdl.s3.sync.destination;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class S3BucketDestinationTest {

    private static final String PREFIX = "prefix/";
    private AmazonS3 awsClient;
    private AmazonS3Exception exception;
    private Destination destination;

    @Before
    public void setUp() throws Exception {
        awsClient = mock(AmazonS3.class);
        exception = mock(AmazonS3Exception.class);
        destination = S3BucketDestination.builder()
                .awsClient(awsClient)
                .prefix(PREFIX)
                .build();
    }

    @Test(expected = DestinationOperationException.class)
    public void startS3SyncSessionThrowsDestinationOperationException() throws DestinationOperationException {
        doThrow(exception).when(awsClient).putObject(any(), any(), anyString());
        destination.startS3SyncSession();
    }

    @Test(expected = DestinationOperationException.class)
    public void stopS3SyncSessionThrowsDestinationOperationException() throws DestinationOperationException {
        doThrow(exception).when(awsClient).putObject(any(), any(), anyString());
        destination.stopS3SyncSession();
    }

    @Test(expected = DestinationOperationException.class)
    public void initUploadingThrowsDestinationOperationException() throws DestinationOperationException {
        doThrow(exception).when(awsClient).initiateMultipartUpload(any());
        destination.initUploading("");
    }

    @Test(expected = DestinationOperationException.class)
    public void getAlreadyUploadedPartsThrowsDestinationOperationExceptionWhenListMultipartUploadsThrowsException() throws DestinationOperationException {
        doThrow(exception).when(awsClient).listMultipartUploads(any());
        destination.getAlreadyUploadedParts("");
    }

    @Test
    public void getAlreadyUploadedPartsRunsNormalWhenNextListingThrowsException() throws DestinationOperationException {
        MultipartUploadListing listing = mock(MultipartUploadListing.class);
        when(listing.isTruncated()).thenReturn(true);
        when(awsClient.listMultipartUploads(any()))
                .thenReturn(listing)
                .thenThrow(exception);

        assertNull(destination.getAlreadyUploadedParts(""));
    }

    @Test
    public void getAlreadyUploadedPartsRunsNormalWhenStreamNextListingThrowsException() throws DestinationOperationException {
        MultipartUploadListing listing = mock(MultipartUploadListing.class);
        when(listing.isTruncated()).thenReturn(false);
        when(awsClient.listMultipartUploads(any()))
                .thenReturn(listing)
                .thenThrow(exception);

        assertNull(destination.getAlreadyUploadedParts(""));
    }

    @Test(expected = DestinationOperationException.class)
    public void commitMultipartUploadThrowsDestinationOperationException() throws DestinationOperationException {
        doThrow(exception).when(awsClient).completeMultipartUpload(any());
        List<PartETag> eTags = Arrays.asList(mock(PartETag.class), mock(PartETag.class));
        destination.commitMultipartUpload("", eTags, "");
    }

    @Test(expected = DestinationOperationException.class)
    public void uploadMultiPartThrowsDestinationOperationException() throws DestinationOperationException {
        UploadPartRequest request = mock(UploadPartRequest.class);
        doThrow(exception).when(awsClient).uploadPart(any());
        destination.uploadMultiPart(request);
    }

    @Test
    public void createUploadPartRequest() throws DestinationOperationException {
        Object newObject = destination.createUploadPartRequest("");
        assertThat(newObject, instanceOf(UploadPartRequest.class));
    }

    @Test
    public void filterUploadableFilesShouldAcceptAllIfS3DirectoryIsEmpty() throws DestinationOperationException {

        ObjectListing listing = mock(ObjectListing.class);
        List<S3ObjectSummary> summaries = new ArrayList<>();
        doReturn(summaries).when(listing).getObjectSummaries();
        doReturn(false).when(listing).isTruncated();
        doReturn(null).when(listing).getNextMarker();

        doReturn(listing).when(awsClient).listObjects((ListObjectsRequest) any());

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
                PREFIX + "file1.txt",
                PREFIX + "subdir/file2.txt",
                PREFIX + "file6.txt"
        );

        List<S3ObjectSummary> summaries = existingPaths.stream()
                .map(path -> {
                    S3ObjectSummary summary = mock(S3ObjectSummary.class);
                    doReturn(path).when(summary).getKey();
                    return summary;
                }).collect(Collectors.toList());

        doReturn(summaries).when(listing).getObjectSummaries();
        doReturn(false).when(listing).isTruncated();
        doReturn(null).when(listing).getNextMarker();

        doReturn(listing).when(awsClient).listObjects((ListObjectsRequest) any());

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

    @Test
    public void filterUploadableFilesShouldHandleMultipleMarkers() throws DestinationOperationException {

        ObjectListing listing = mock(ObjectListing.class);

        List<String> existingPaths = Arrays.asList(
                PREFIX + "file1.txt",
                PREFIX + "subdir/file2.txt",
                PREFIX + "file6.txt",
                PREFIX + "file7.txt",
                PREFIX + "file8.txt",
                PREFIX + "file9.txt"
        );

        List<S3ObjectSummary> summaries = existingPaths.stream()
                .map(path -> {
                    S3ObjectSummary summary = mock(S3ObjectSummary.class);
                    doReturn(path).when(summary).getKey();
                    return summary;
                }).collect(Collectors.toList());

        List<S3ObjectSummary> summaries1 = summaries.subList(0, 3);
        List<S3ObjectSummary> summaries2 = summaries.subList(3, 6);

        when(listing.getObjectSummaries())
                .thenReturn(summaries1)
                .thenReturn(summaries2);

        when(listing.isTruncated())
                .thenReturn(true)
                .thenReturn(false);

        when(listing.getNextMarker())
                .thenReturn("1")
                .thenReturn(null);

        doReturn(listing)
                .when(awsClient)
                .listObjects((ListObjectsRequest) any());

        List<String> paths = Arrays.asList(
                "file1.txt",
                "subdir/file2.txt",
                "file3.txt",
                "subdir/file4.txt",
                "file5.txt",
                "file6.txt",
                "file7.txt",
                "file8.txt",
                "file9.txt",
                "file10.txt",
                "file11.txt"
        );
        List<String> result = destination.filterUploadableFiles(paths);
        List<String> expected = Arrays.asList(
                "file3.txt",
                "subdir/file4.txt",
                "file5.txt",
                "file10.txt",
                "file11.txt"    
        );
        Collections.sort(result);
        Collections.sort(expected);
        assertEquals(result, expected);
    }
}
