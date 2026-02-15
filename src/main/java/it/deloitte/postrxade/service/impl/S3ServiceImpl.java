package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.service.S3Service;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of S3Service using AWS SDK v2.
 * Uses IAM Role for authentication.
 */
@Service
@Slf4j
public class S3ServiceImpl implements S3Service {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3ServiceImpl.class);

    @Autowired
    private S3Client s3Client;

    @Autowired
    private String s3BucketName;

    @Value("${aws.s3.input-folder}")
    private String s3InputFolder;

    @Value("${aws.s3.input-folder-loaded}")
    private String s3InputFolderLoaded;

    @Value("${aws.s3.output-folder}")
    private String outputFolder;

    /**
     * Verifica che il S3Client sia disponibile e funzionante.
     * Se non lo è, tenta di ricreare le credenziali.
     */
    private void ensureS3ClientAvailable() {
        if (s3Client == null) {
            LOGGER.error("S3Client is null!");
            throw new IllegalStateException("S3Client is not available");
        }
        LOGGER.debug("S3Client is available, bucket: {}", s3BucketName);
    }

    @Override
    public String uploadFile(String key, InputStream inputStream, String contentType) {
        try {
            LOGGER.info("Uploading file to S3: s3://{}/{}", s3BucketName, key);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(key)
                    .contentType(contentType)
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, inputStream.available()));

            LOGGER.info("File uploaded successfully to S3: s3://{}/{}", s3BucketName, key);
            return key;

        } catch (S3Exception e) {
            LOGGER.error("Error uploading file to S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to upload file to S3", e);
        } catch (IOException e) {
            LOGGER.error("Error reading input stream: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to read input stream", e);
        }
    }

    @Override
    public Resource downloadFile(String key) {
        try {
            LOGGER.info("Downloading file from S3: s3://{}/{}", s3BucketName, key);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(key)
                    .build();

            InputStream inputStream = s3Client.getObject(getObjectRequest);

            return new InputStreamResource(inputStream) {
                @Override
                public String getDescription() {
                    return "S3 file: " + key;
                }
            };

        } catch (NoSuchKeyException e) {
            LOGGER.warn("File not found in S3: s3://{}/{}", s3BucketName, key);
            throw new RuntimeException("File not found: " + key, e);
        } catch (S3Exception e) {
            LOGGER.error("Error downloading file from S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to download file from S3", e);
        }
    }

    @Override
    public InputStream downloadFileAsStream(String key) {
        try {
            LOGGER.info("Downloading file as stream from S3: s3://{}/{}", s3BucketName, key);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(key)
                    .build();

            return s3Client.getObject(getObjectRequest);

        } catch (NoSuchKeyException e) {
            LOGGER.warn("File not found in S3: s3://{}/{}", s3BucketName, key);
            throw new RuntimeException("File not found: " + key, e);
        } catch (S3Exception e) {
            LOGGER.error("Error downloading file from S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to download file from S3", e);
        }
    }

    public InputStream downloadFileAsStreamTest(String fileName) throws IOException {
        ClassPathResource resource = new ClassPathResource("original_input/" + fileName);

        return resource.getInputStream();
    }

    @Override
    public List<S3Object> listObjects(String prefix) {
        ensureS3ClientAvailable();
        try {
            LOGGER.info("Listing objects in S3 bucket: {} with prefix: {}", s3BucketName, prefix);

            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(s3BucketName)
                    .prefix(prefix)
                    .build();

            ListObjectsV2Response response = s3Client.listObjectsV2(listObjectsRequest);
            List<S3Object> objects = new ArrayList<>();

            for (software.amazon.awssdk.services.s3.model.S3Object s3Object : response.contents()) {
                objects.add(s3Object);
            }

            LOGGER.info("Found {} objects in S3 bucket with prefix: {}", objects.size(), prefix);
            return objects;

        } catch (S3Exception e) {
            LOGGER.error("Error listing objects in S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to list objects in S3", e);
        }
    }

    @Override
    public void deleteFile(String key) {
        try {
            LOGGER.info("Deleting file from S3: s3://{}/{}", s3BucketName, key);

            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(key)
                    .build();

            s3Client.deleteObject(deleteObjectRequest);

            LOGGER.info("File deleted successfully from S3: s3://{}/{}", s3BucketName, key);

        } catch (S3Exception e) {
            LOGGER.error("Error deleting file from S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to delete file from S3", e);
        }
    }

    @Override
    public boolean fileExists(String key) {
        try {
            LOGGER.debug("Checking if file exists in S3: s3://{}/{}", s3BucketName, key);

            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(key)
                    .build();

            s3Client.headObject(headObjectRequest);
            return true;

        } catch (NoSuchKeyException e) {
            LOGGER.debug("File does not exist in S3: s3://{}/{}", s3BucketName, key);
            return false;
        } catch (S3Exception e) {
            LOGGER.error("Error checking file existence in S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to check file existence in S3", e);
        }
    }

    @Override
    public String getBucketName() {
        return s3BucketName;
    }

    private void moveFile(String sourceKey, String destinationKey) {
        LOGGER.info("START: Move operation | Source: s3://{}/{} | Destination: s3://{}/{}",
                s3BucketName, sourceKey, s3BucketName, destinationKey);

        try {
            String copySource = String.format("%s/%s", s3BucketName, sourceKey);

            LOGGER.debug("Attempting CopyObject with copySource: {}", copySource);

            CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                    .copySource(copySource)
                    .destinationBucket(s3BucketName)
                    .destinationKey(destinationKey)
                    .build();

            CopyObjectResponse copyResponse = s3Client.copyObject(copyRequest);

            if (copyResponse.copyObjectResult() != null) {
                LOGGER.info("SUCCESS: Copy phase completed. ETag: {}", copyResponse.copyObjectResult().eTag());

                LOGGER.debug("Attempting DeleteObject for sourceKey: {}", sourceKey);

                DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                        .bucket(s3BucketName)
                        .key(sourceKey)
                        .build();

                s3Client.deleteObject(deleteRequest);

                LOGGER.info("FINISH: Move operation completed successfully.");
            } else {
                LOGGER.error("FAILURE: Copy phase returned null result for {}", sourceKey);
                throw new RuntimeException("Copy operation failed: Empty response result");
            }
        } catch (S3Exception e) {
            LOGGER.error("AWS S3 ERROR: [{}] {} | Request ID: {}",
                    e.awsErrorDetails().errorCode(),
                    e.awsErrorDetails().errorMessage(),
                    e.requestId());
            throw new RuntimeException("Failed to move file in S3", e);
        } catch (Exception e) {
            LOGGER.error("UNEXPECTED ERROR during S3 move: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void moveFileFromInputToInputLoaded(String fileNameOrKey) {
        LOGGER.info("Resolution: Moving filename/key [{}] using folders [input: {}] -> [loaded: {}]",
                fileNameOrKey, s3InputFolder, s3InputFolderLoaded);

        // Extract just the filename if a full key path is provided
        String fileName = fileNameOrKey;
        if (fileNameOrKey.contains("/")) {
            fileName = fileNameOrKey.substring(fileNameOrKey.lastIndexOf("/") + 1);
            LOGGER.debug("Extracted filename '{}' from key '{}'", fileName, fileNameOrKey);
        }

        String sourceKey = String.format("%s/%s", s3InputFolder.replaceAll("/$", ""), fileName);
        String destinationKey = String.format("%s/%s", s3InputFolderLoaded.replaceAll("/$", ""), fileName);

        moveFile(sourceKey, destinationKey);
    }

    @Override
    public List<String> fetchFileKeysFromBucket() throws NotFoundRecordException {
        log.debug("Fetching file keys from S3 bucket, input folder: {}", s3InputFolder);

        List<S3Object> s3Objects = listObjects(s3InputFolder);

        List<String> keys = s3Objects.stream()
                .map(S3Object::key)
                .filter(key -> !key.equals(s3InputFolder) && !key.endsWith("/"))
                .collect(Collectors.toList());

        if (keys.isEmpty()) {
            log.warn("No files found in S3 input folder: {}", s3InputFolder);
            throw new NotFoundRecordException("No files found in S3 input folder: " + s3InputFolder);
        }

        log.debug("Found {} file key(s) to process", keys.size());
        return keys;
    }

    @Override
    public List<String> fetchFileKeysFromBucketTest() throws NotFoundRecordException {
        List<String> filenames = new ArrayList<>();

        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

        try {
            Resource[] resources = resolver.getResources("classpath:original_input/*");
            for (Resource resource : resources) {
                filenames.add(resource.getFilename());
            }
        } catch (Exception e) {
        }

        return filenames;
    }
}
