package tdl.s3.upload;

import java.io.File;
import java.io.IOException;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;
import tdl.s3.sync.progress.ProgressListener;

public class LocalFileUploadingStrategy implements FileUploadingStrategy {

    @Override
    public void setDestination(Destination destination) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void upload(File file, String remotePath) throws DestinationOperationException, IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setListener(ProgressListener listener) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
