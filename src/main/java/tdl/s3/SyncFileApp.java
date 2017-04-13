package tdl.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import tdl.s3.upload.FileUploadingService;

import java.io.File;
import java.util.Scanner;

public class SyncFileApp {

    @Parameter(names={"--access-key", "-a"}, required = true)
    private String accessKey;

    @Parameter(names={"--secret-key", "-s"}, required = true)
    private String secretKey;

    @Parameter(names={"--region", "-r"}, required = true)
    private String region;

    @Parameter(names={"--bucket", "-b"}, required = true)
    private String bucket;

    @Parameter(names={"--file", "-f"}, required = true)
    private String filePath;

    public static void main(String[] args) {
        SyncFileApp main = new SyncFileApp();

        JCommander jCommander = new JCommander(main, args);

        main.run();
    }

    public void run() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();
        FileUploadingService fileUploadingService = new FileUploadingService(amazonS3, bucket);

        File file = new File(filePath);
        fileUploadingService.upload(file);
    }

    private static String getArg(String message) {
        System.out.println(message);
        Scanner scanner = new Scanner(System.in);
        return scanner.nextLine();
    }
}
