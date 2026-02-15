package it.deloitte.postrxade.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Configuration for AWS S3 integration.
 * <p>
 * Uses DefaultCredentialsProvider which will try credentials in this order:
 * 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * 2. AWS credentials file (~/.aws/credentials)
 * 3. IAM Role (when running on EC2/ECS/App Runner)
 */
@Configuration
public class AwsS3Config {

    private static final Logger LOGGER = LoggerFactory.getLogger(AwsS3Config.class);

    @Value("${aws.s3.region}")
    private String region;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    /**
     * Creates S3Client using DefaultCredentialsProvider.
     * <p>
     * In production (App Runner): Uses IAM Role automatically via InstanceProfileCredentialsProvider
     * In local development: Uses AWS credentials from environment variables or ~/.aws/credentials
     * <p>
     * The DefaultCredentialsProvider chain will try:
     * 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
     * 2. Java system properties
     * 3. Web identity token from environment or container
     * 4. Shared credentials file (~/.aws/credentials)
     * 5. Container credentials (ECS)
     * 6. Instance profile credentials (EC2/App Runner via IMDS)
     */
    @Bean
    public S3Client s3Client() {
        LOGGER.info("Creating S3Client for region: {}, bucket: {}", region, bucketName);
        
        try {
            // DefaultCredentialsProvider gestisce automaticamente la catena di credenziali
            // e funziona anche nei thread asincroni perché accede all'IMDS a livello di istanza
            // Il provider viene ricreato ad ogni chiamata per assicurarsi che le credenziali siano sempre fresche
            DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
            
            // Configurazione del client con retry policy per gestire meglio i problemi di credenziali
            ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.defaultRetryPolicy())
                    .build();
            
            S3Client client = S3Client.builder()
                    .region(Region.of(region))
                    .credentialsProvider(credentialsProvider)
                    .overrideConfiguration(clientConfig)
                    .build();
            
            LOGGER.info("S3Client created successfully with DefaultCredentialsProvider and retry policy");
            return client;
        } catch (Exception e) {
            LOGGER.error("Failed to create S3Client. Make sure AWS credentials are configured.", e);
            throw new RuntimeException("Failed to create S3Client. Please configure AWS credentials.", e);
        }
    }

    /**
     * Provides the S3 bucket name as a bean for injection.
     */
    @Bean
    public String s3BucketName() {
        return bucketName;
    }
}
