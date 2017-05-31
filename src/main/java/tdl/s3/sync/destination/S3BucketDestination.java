package tdl.s3.sync.destination;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.util.ArrayList;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.upload.MultipartUploadResult;
import tdl.s3.upload.FileUploadingStrategy;

@Builder
@Slf4j
public class S3BucketDestination implements Destination {

    private final AmazonS3 awsClient;

    private final String bucket;

    private final String prefix;

    @Override
    public List<String> filterUploadableFiles(List<String> paths) throws DestinationOperationException {
        Set<String> existingItems = listAllObjects().stream()
                .map(summary -> summary.getKey())
                .collect(Collectors.toSet());

        int trimLength = prefix.length();
        return paths.stream()
                .map(path -> prefix + path)
                .filter(path -> !existingItems.contains(path))
                .map(path -> path.substring(trimLength))
                .collect(Collectors.toList());
    }

    private Set<S3ObjectSummary> listAllObjects() {
        ListObjectsRequest request = new ListObjectsRequest()
                .withBucketName(bucket)
                .withPrefix(prefix);
        ObjectListing result;
        Set<S3ObjectSummary> summaries = new HashSet<>();
        do {
            result = awsClient.listObjects(request);
            request.setMarker(result.getNextMarker());
            summaries.addAll(result.getObjectSummaries());
        } while (result.isTruncated());
        return summaries;
    }

    @Override
    public FileUploadingStrategy createUploadingStrategy() {
        
    }

}
