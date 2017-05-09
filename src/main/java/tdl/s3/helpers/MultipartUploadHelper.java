package tdl.s3.helpers;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
}
