/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tdl.s3.sync;

import java.io.File;

/**
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
public class DummyProgressListener implements ProgressListener {

    @Override
    public void uploadFileStarted(File file, String uploadId) {
        //DO NOTHING
    }

    @Override
    public void uploadFileProgress(String uploadId, long uploadedByte) {
        //DO NOTHING
    }

    @Override
    public void uploadFileFinished(File file) {
        //DO NOTHING
    }

}
