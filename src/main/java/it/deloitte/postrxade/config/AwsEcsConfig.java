package it.deloitte.postrxade.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecs.EcsClient;

/**
 * Configuration for AWS ECS integration.
 * <p>
 * Uses DefaultCredentialsProvider which will try credentials in this order:
 * 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * 2. AWS credentials file (~/.aws/credentials)
 * 3. IAM Role (when running on EC2/ECS/App Runner)
 * <p>
 * This configuration is only loaded in the main application (dev/local/prod profiles),
 * NOT in batch or output profiles, as those ECS tasks don't need to launch other tasks.
 */
@Configuration
@Profile("!batch & !output")  // Exclude batch and output profiles - they don't need to launch ECS tasks
public class AwsEcsConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(AwsEcsConfig.class);

    @Value("${aws.region:eu-central-1}")
    private String region;

    /**
     * Creates EcsClient using DefaultCredentialsProvider.
     * <p>
     * In production (App Runner/ECS): Uses IAM Role automatically via InstanceProfileCredentialsProvider
     * In local development: Uses AWS credentials from environment variables or ~/.aws/credentials
     * <p>
     * This bean is only created if aws.ecs.enabled is true (default: true).
     * If ECS is disabled or credentials are not available, the bean will not be created
     * and EcsTaskService will handle the missing client gracefully.
     */
    @Bean
    @Lazy
    @ConditionalOnProperty(name = "aws.ecs.enabled", havingValue = "true", matchIfMissing = true)
    public EcsClient ecsClient() {
        LOGGER.info("Creating EcsClient for region: {}", region);
        
        try {
            DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
            
            ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.defaultRetryPolicy())
                    .build();
            
            EcsClient client = EcsClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(credentialsProvider)
                    .overrideConfiguration(clientConfig)
                    .build();
            
            LOGGER.info("EcsClient created successfully with DefaultCredentialsProvider and retry policy");
            return client;
        } catch (Exception e) {
            LOGGER.error("Failed to create EcsClient. ECS functionality will not be available. " +
                    "Error: {}. To disable ECS, set aws.ecs.enabled=false", e.getMessage(), e);
            // Re-throw to prevent bean creation - this will make EcsTaskService handle missing client
            throw new RuntimeException("EcsClient initialization failed. Set aws.ecs.enabled=false to disable ECS.", e);
        }
    }
}
