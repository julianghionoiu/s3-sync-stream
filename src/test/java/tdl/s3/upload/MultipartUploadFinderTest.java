package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tdl.s3.sync.destination.DestinationOperationException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class MultipartUploadFinderTest {

    @Test
    public void getAlreadyStartedMultipartUploads() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        MultipartUploadListing listing = mock(MultipartUploadListing.class);
        when(awsClient.listMultipartUploads(any()))
                .thenReturn(listing);
        String bucket = "bucket";
        String prefix = "prefix";
        MultipartUploadFinder finder = new MultipartUploadFinder(awsClient, bucket, prefix);
        finder.getAlreadyStartedMultipartUploads();
    }

    @Test
    public void getAlreadyStartedMultipartUploadShouldThrowDestinationOperationException() throws DestinationOperationException {
        DestinationOperationException destinationOperationException = Assertions.assertThrows(DestinationOperationException.class, () -> {

            AmazonS3 awsClient = mock(AmazonS3.class);

            when(awsClient.listMultipartUploads(any()))
                    .thenThrow(new AmazonS3Exception("Message"));
            String bucket = "bucket";
            String prefix = "prefix";
            MultipartUploadFinder finder = new MultipartUploadFinder(awsClient, bucket, prefix);
            finder.getAlreadyStartedMultipartUploads();
        });

        MatcherAssert.assertThat(destinationOperationException.getMessage(), containsString("Failed to list upload request:"));

    }

    @Test
    public void getAlreadyStartedMultipartUploadsShouldIterateMultipleListing() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        MultipartUploadListing listing = mock(MultipartUploadListing.class);

        when(listing.isTruncated())
                .thenReturn(true)
                .thenReturn(false);

        when(awsClient.listMultipartUploads(any()))
                .thenReturn(listing);

        String bucket = "bucket";
        String prefix = "prefix";
        MultipartUploadFinder finder = new MultipartUploadFinder(awsClient, bucket, prefix);
        finder.getAlreadyStartedMultipartUploads();
    }
    
    @Test
    public void getAlreadyStartedMultipartUploadsShouldIterateMultipleListingWithException() throws DestinationOperationException {
        AmazonS3 awsClient = mock(AmazonS3.class);
        MultipartUploadListing listing = mock(MultipartUploadListing.class);

        when(listing.isTruncated())
                .thenReturn(true)
                .thenReturn(false);

        when(awsClient.listMultipartUploads(any()))
                .thenReturn(listing)
                .thenThrow(new AmazonS3Exception("Message"));

        String bucket = "bucket";
        String prefix = "prefix";
        MultipartUploadFinder finder = new MultipartUploadFinder(awsClient, bucket, prefix);
        finder.getAlreadyStartedMultipartUploads();
    }
}
