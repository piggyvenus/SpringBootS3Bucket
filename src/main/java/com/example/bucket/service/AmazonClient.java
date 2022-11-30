package com.example.bucket.service;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.client.builder.AwsClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.S3ClientOptions;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.io.ByteArrayInputStream;
import java.util.List;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import java.io.ByteArrayInputStream;


@Service
public class AmazonClient {

    private AmazonS3 s3client;

    @Value("${amazonProperties.endpointUrl}")
    private String endpointUrl;
    @Value("${amazonProperties.bucketName}")
    private String bucketName;
    @Value("${amazonProperties.accessKey}")
    private String accessKey;
    @Value("${amazonProperties.secretKey}")
    private String secretKey;
    @Value("${amazonProperties.regionName}")
    private String regionName;

    private String keyname;


    @PostConstruct
    private void initializeAmazon() {

        AWSCredentials credentials = new BasicAWSCredentials(this.accessKey, this.secretKey);
        //this.s3client = new AmazonS3Client(credentials);
        /*
        this.s3client = AmazonS3ClientBuilder.standard()
                    .withCredentials(
                            new AWSStaticCredentialsProvider(credentials))
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration("https://s3-openshift-storage.apps.cluster-x2ckn.x2ckn.sandbox400.opentlc.com", this.regionName))
                    .build();
         */
        this.s3client = AmazonS3ClientBuilder.standard()
                    .withCredentials(
                            new AWSStaticCredentialsProvider(credentials))
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration("https://s3-openshift-storage.apps.cluster-x2ckn.x2ckn.sandbox400.opentlc.com",this.regionName))
                    .build();

    }

    public String uploadFile(MultipartFile multipartFile) {
        String fileUrl = "";
        try {
            File file = convertMultiPartToFile(multipartFile);
            String fileName = generateFileName(multipartFile);
            System.out.println("fileName: "+fileName);
            this.keyname = fileName;
            fileUrl = endpointUrl + "/" + bucketName + "/" + fileName;
            System.out.println("fileURL: "+fileUrl);
            uploadFileTos3bucket(fileName, file);
            file.delete();
        } catch (Exception e) {
           e.printStackTrace();
        }
        return fileUrl;
    }

    private File convertMultiPartToFile(MultipartFile file) throws IOException {
        File convFile = new File(file.getOriginalFilename());
        FileOutputStream fos = new FileOutputStream(convFile);
        fos.write(file.getBytes());
        fos.close();
        return convFile;
    }

    private String generateFileName(MultipartFile multiPart) {
        return new Date().getTime() + "-" + multiPart.getOriginalFilename().replace(" ", "_");
    }

    private void uploadFileTos3bucket(String fileName, File file) {

        System.out.println("Updating file...");

        try {
            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.HTTP);
            AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials(this.accessKey, this.secretKey), config);
            S3ClientOptions options =  new S3ClientOptions();
            options.setPathStyleAccess(true);
            s3.setS3ClientOptions(options);
            s3.setEndpoint("s3-openshift-storage.apps.cluster-x2ckn.x2ckn.sandbox400.opentlc.com");

            //String newobjfile = "/Users/shannachan/projects/s3/image002.png";
            System.out.println("step 1...");
            PutObjectRequest request = new PutObjectRequest(this.bucketName, fileName, file);
            System.out.println("step 2...");
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("text/plain;charset=UTF-8");
            System.out.println("step 3...");
            metadata.addUserMetadata("title", "someTitle");
            System.out.println("step 4...");
            request.setMetadata(metadata);
            System.out.println("step 5...");
            s3.putObject(request);
            System.out.println("step 6...");

        }catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

    public String deleteFileFromS3Bucket(String fileUrl) {
        String fileName = fileUrl.substring(fileUrl.lastIndexOf("/") + 1);
        s3client.deleteObject(new DeleteObjectRequest(bucketName, fileName));
        return "Successfully deleted";
    }

    public String listBuckets(String odf) {
        List<Bucket> buckets = s3client.listBuckets();
        System.out.println("Your Amazon S3 bsuckets are:");
        for (Bucket b : buckets) {
            System.out.println("* " + b.getName());
        }
        return "Successfully list all bucket";
    }


}
