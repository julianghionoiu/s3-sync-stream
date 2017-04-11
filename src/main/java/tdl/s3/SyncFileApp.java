package tdl.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import tdl.s3.upload.FileUploadingService;

import java.io.File;
import java.util.Scanner;

public class SyncFileApp {

    public static void main(String[] args) {
        if (args.length == 0) System.out.println("Specify file to upload please.");

        String region = getArg("Specify AWS region...");//"eu-west-1";
        String accessKey = getArg("Specify AWS access key...");//"AKIAJJ3E7NQI3EVXR5RA";
        String secretKey = getArg("Specify AWS secret key...");//"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";
        String filePath = getArg("Specify file to upload...");//"/home/vdanyliuk/Музика/AC_DC/1979 - Highway To Hell/09. Love Hungry Man.mp3";
        File file = new File(filePath);

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();
        FileUploadingService fileUploadingService = new FileUploadingService(amazonS3, "test-bucket-ertretgsdfgsdfg");

        fileUploadingService.upload(file);
    }

    private static String getArg(String message) {
        System.out.println(message);
        Scanner scanner = new Scanner(System.in);
        return scanner.nextLine();
    }
}
