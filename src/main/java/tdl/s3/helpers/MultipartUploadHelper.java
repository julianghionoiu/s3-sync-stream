package tdl.s3.helpers;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MultipartUploadHelper {

    public static List<PartETag> getPartETagsFromPartListing(PartListing listing) {
        Stream<PartSummary> partSummaryStream = Optional.ofNullable(listing)
                .map(PartListing::getParts)
                .map(Collection::stream)
                .orElseGet(Stream::empty);

        return partSummaryStream.map(partSummary -> new PartETag(partSummary.getPartNumber(), partSummary.getETag()))
                .collect(Collectors.toList());
    }

    public static long getUploadedSize(PartListing partListing) {
        return partListing.getParts().stream()
                .mapToLong(PartSummary::getSize)
                .sum();
    }

    public static int getLastPartIndex(PartListing partListing) {
        return partListing.getParts()
                .stream()
                .mapToInt(PartSummary::getPartNumber)
                .max()
                .orElse(1);
    }
    
    public static Set<Integer> getFailedMiddlePartNumbers(PartListing partListing) {
        AtomicInteger lastPartNumber = new AtomicInteger(0);
        Set<Integer> uploadedParts = partListing.getParts().stream()
                .map(PartSummary::getPartNumber)
                .peek(n -> {
                    if (lastPartNumber.get() < n) {
                        lastPartNumber.set(n);
                    }
                })
                .collect(Collectors.toSet());

        return IntStream.range(1, lastPartNumber.get())
                .filter(n -> !uploadedParts.contains(n))
                .boxed()
                .collect(Collectors.toSet());
    }
}
