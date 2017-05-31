package tdl.s3.sync.destination;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.util.List;
import java.util.Map;
import tdl.s3.upload.MultipartUploadResult;
import tdl.s3.upload.FileUploadingStrategy;

public class LocalDestination implements Destination {

    @Override
    public List<String> filterUploadableFiles(List<String> paths) throws DestinationOperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public FileUploadingStrategy createUploadingStrategy() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
