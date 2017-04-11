package tdl.s3;

import org.junit.Test;

public class C_OnDemand_IncompleteFileUpload_AccTest {

    /**
     * NOTES
     *
     * This is where it gets interesting. :)
     * We want to start uploading the video recording as soon as it starts being generated.
     *
     * I am using https://github.com/julianghionoiu/dev-screen-record to generate the recording and
     * it is configured such that information is always appended to the end of the video and never changed.
     * Internally, the video recording is composed of small independent parts and can be played even if the
     * recording has not stopped.
     * This way we can upload chunks of video as it is being generated.
     *
     * The tricky bit is to detect when the recording has completed and the upload can be finalised.
     */


    @Test
    public void should_upload_incomplete_file() throws Exception {
        //Given there is video recording in progress ( we have an incomplete file )

        //When I trigger a file upload for the incomplete file

        //Then S3 should contain the existing data
    }


    @Test
    public void should_be_able_to_continue_incomplete_file_and_finalise() throws Exception {
        //Given S3 we have an incomplete file in S3

        //When I trigger a file upload for the complete file
        //TODO What is the difference between an incomplete and a complete file

        //Then S3 should contain the complete data
    }

}