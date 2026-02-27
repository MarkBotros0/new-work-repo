package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.service.EcsTaskService;
import it.deloitte.postrxade.tenant.TenantConfiguration;
import it.deloitte.postrxade.tenant.TenantConfiguration.TenantProperties;
import it.deloitte.postrxade.tenant.TenantContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service implementation for launching ECS tasks.
 * <p>
 * Multi-tenant: quando si lancia il task di output generation, si usa il tenant corrente (da sessione/URL).
 * Il task ECS riceve TENANT_ID, DB_* e S3_BUCKET_* del tenant così l'output viene generato sul DB e nella
 * cartella S3 corretti (es. nexi → NEXI/OUTPUT, amex → AMEX/OUTPUT).
 * <p>
 * This service is only available in the main application (dev/local/prod profiles),
 * NOT in batch or output profiles.
 */
@Slf4j
@Service
@Profile("!batch & !output")  // Exclude batch and output profiles - they don't need to launch ECS tasks
public class EcsTaskServiceImpl implements EcsTaskService {

    private static final Pattern JDBC_URL_PATTERN = Pattern.compile("jdbc:mariadb://([^:/]+)(?::(\\d+))?/([^?]+)");

    @Autowired(required = false)
    private EcsClient ecsClient;

    @Autowired
    private TenantConfiguration tenantConfiguration;

    @Value("${aws.ecs.cluster-name:}")
    private String clusterName;

    @Value("${aws.ecs.task-definition-output:}")
    private String taskDefinitionName;

    @Value("${aws.ecs.container-name-output:output-generation}")
    private String containerName;

    @Value("${aws.ecs.subnet-ids:}")
    private String subnetIds;

    @Value("${aws.ecs.security-group-ids:}")
    private String securityGroupIds;

    @Value("${aws.ecs.launch-type:FARGATE}")
    private String launchType;

    @Value("${aws.ecs.output-rows-per-file:}")
    private String outputRowsPerFile; // Optional: if set, will override task definition default

    @Value("${aws.s3.bucket-name:}")
    private String s3BucketName;

    @Value("${aws.s3.region:eu-central-1}")
    private String s3BucketRegion;

    @Override
    public String launchOutputGenerationTask(Long submissionId) {
        // Multi-tenant: tenant corrente (da sessione/URL) determina DB e cartella S3 del task
        String tenantId = TenantContext.getTenantId();
        if (tenantId == null || tenantId.isBlank()) {
            tenantId = tenantConfiguration.getBootstrapTenantResolved();
            log.warn("No tenant in context, using bootstrap tenant for output task: {}", tenantId);
        } else {
            tenantId = TenantConfiguration.resolveTenantAlias(tenantId);
        }

        TenantProperties tenantProps = tenantConfiguration.getTenantProperties(tenantId);
        if (tenantProps == null) {
            throw new IllegalStateException("Tenant '" + tenantId + "' is not configured. Cannot launch output generation task.");
        }

        log.info("Launching ECS task for output generation - submissionId: {}, tenant: {}", submissionId, tenantId);

        // Validate ECS client is available
        if (ecsClient == null) {
            throw new IllegalStateException("EcsClient is not available. ECS functionality is disabled. " +
                    "Ensure AWS credentials are configured and aws.ecs.enabled=true.");
        }

        // Validate required configuration
        if (clusterName == null || clusterName.isEmpty()) {
            throw new IllegalStateException("AWS ECS cluster-name is not configured. Set aws.ecs.cluster-name property.");
        }
        if (taskDefinitionName == null || taskDefinitionName.isEmpty()) {
            throw new IllegalStateException("AWS ECS task-definition-output is not configured. Set aws.ecs.task-definition-output property.");
        }
        // Con FARGATE (awsvpc) la network configuration è obbligatoria
        if ("FARGATE".equalsIgnoreCase(launchType != null ? launchType.trim() : "")) {
            if (subnetIds == null || subnetIds.isBlank()) {
                throw new IllegalStateException(
                        "Per lanciare il task ECS output con FARGATE serve la network configuration. " +
                        "Imposta aws.ecs.subnet-ids (es. variabile d'ambiente AWS_ECS_SUBNET_IDS con gli ID delle subnet separate da virgola).");
            }
            if (securityGroupIds == null || securityGroupIds.isBlank()) {
                throw new IllegalStateException(
                        "Per lanciare il task ECS output con FARGATE serve la network configuration. " +
                        "Imposta aws.ecs.security-group-ids (es. variabile d'ambiente AWS_ECS_SECURITY_GROUP_IDS con gli ID dei security group separati da virgola).");
            }
        }

        try {
            // Parse DB host/port from tenant's database URL
            DbEnv dbEnv = parseJdbcUrl(tenantProps.getDatabaseUrl());
            if (dbEnv == null) {
                throw new IllegalStateException("Cannot parse database URL for tenant " + tenantId);
            }

            // Nome container: deve coincidere con quello nella task definition ECS (case-sensitive)
            String effectiveContainerName = (containerName != null && !containerName.isBlank()) ? containerName.trim() : "output-generation";
            if (effectiveContainerName.equals("output-generation") && (containerName == null || containerName.isBlank())) {
                log.debug("aws.ecs.container-name-output non impostato, uso default: output-generation");
            }

            // Cartelle output per tenant: NEXI/OUTPUT, AMEX/OUTPUT (maiuscolo, come su S3)
            String outputFolder = (tenantId != null ? tenantId.toUpperCase() : "OUTPUT") + "/OUTPUT";
            log.info("Output ECS task: containerName={}, S3_BUCKET_OUTPUT_FOLDER={}, tenant={} (il container nella task definition deve chiamarsi esattamente così, stesso case)",
                    effectiveContainerName, outputFolder, tenantId);

            // Prepare environment variables (tenant-specific so ECS task uses correct DB and S3 folder)
            Map<String, String> environmentVariables = new HashMap<>();
            environmentVariables.put("TENANT_ID", tenantId);
            environmentVariables.put("SUBMISSION_ID", String.valueOf(submissionId));
            environmentVariables.put("SPRING_PROFILES_ACTIVE", "output");
            environmentVariables.put("DB_HOST", dbEnv.host);
            environmentVariables.put("DB_PORT", String.valueOf(dbEnv.port));
            environmentVariables.put("DB_NAME", tenantProps.getDatabaseName() != null ? tenantProps.getDatabaseName() : dbEnv.databaseName);
            environmentVariables.put("DB_USERNAME", tenantProps.getDatabaseUsername() != null ? tenantProps.getDatabaseUsername() : "");
            environmentVariables.put("DB_PASSWORD", tenantProps.getDatabasePassword() != null ? tenantProps.getDatabasePassword() : "");
            environmentVariables.put("S3_BUCKET_NAME", s3BucketName != null ? s3BucketName : "");
            environmentVariables.put("S3_BUCKET_REGION", s3BucketRegion != null ? s3BucketRegion : "eu-central-1");
            environmentVariables.put("S3_BUCKET_OUTPUT_FOLDER", outputFolder);

            // Add OUTPUT_ROWS_PER_FILE if configured (optional override)
            if (outputRowsPerFile != null && !outputRowsPerFile.trim().isEmpty()) {
                environmentVariables.put("OUTPUT_ROWS_PER_FILE", outputRowsPerFile.trim());
                log.debug("Overriding OUTPUT_ROWS_PER_FILE with value: {}", outputRowsPerFile);
            }

            // Build container overrides
            // IMPORTANTE: .name() deve essere il nome del container nella task definition (case-sensitive)
            ContainerOverride containerOverride = ContainerOverride.builder()
                    .name(effectiveContainerName)
                    .environment(
                            environmentVariables.entrySet().stream()
                                    .map(entry -> KeyValuePair.builder()
                                            .name(entry.getKey())
                                            .value(entry.getValue())
                                            .build())
                                    .toList()
                    )
                    .build();

            // Build network configuration if subnets/security groups are provided
            NetworkConfiguration.Builder networkConfigBuilder = NetworkConfiguration.builder();
            
            if (subnetIds != null && !subnetIds.isEmpty()) {
                String[] subnetArray = subnetIds.split(",");
                networkConfigBuilder.awsvpcConfiguration(
                        AwsVpcConfiguration.builder()
                                .subnets(subnetArray)
                                .assignPublicIp(AssignPublicIp.DISABLED)
                                .securityGroups(securityGroupIds != null && !securityGroupIds.isEmpty() 
                                        ? java.util.Arrays.asList(securityGroupIds.split(","))
                                        : null)
                                .build()
                );
            }

            // Build run task request
            RunTaskRequest.Builder requestBuilder = RunTaskRequest.builder()
                    .cluster(clusterName)
                    .taskDefinition(taskDefinitionName)
                    .launchType(LaunchType.fromValue(launchType))
                    .overrides(TaskOverride.builder()
                            .containerOverrides(containerOverride)
                            .build());

            if (networkConfigBuilder.build().awsvpcConfiguration() != null) {
                requestBuilder.networkConfiguration(networkConfigBuilder.build());
            }

            RunTaskRequest runTaskRequest = requestBuilder.build();

            log.debug("Running ECS task with request: cluster={}, taskDefinition={}, launchType={}",
                    clusterName, taskDefinitionName, launchType);

            RunTaskResponse response = ecsClient.runTask(runTaskRequest);

            if (response.failures() != null && !response.failures().isEmpty()) {
                StringBuilder errorMsg = new StringBuilder("Failed to launch ECS task. Failures: ");
                for (Failure failure : response.failures()) {
                    errorMsg.append(String.format("[%s: %s] ", failure.reason(), failure.detail()));
                }
                log.error(errorMsg.toString());
                throw new RuntimeException(errorMsg.toString());
            }

            if (response.tasks() != null && !response.tasks().isEmpty()) {
                String taskArn = response.tasks().get(0).taskArn();
                log.info("ECS task launched successfully - taskArn: {}, submissionId: {}", taskArn, submissionId);
                return taskArn;
            } else {
                log.error("ECS task launch returned no tasks");
                throw new RuntimeException("ECS task launch returned no tasks");
            }

        } catch (Exception e) {
            log.error("Error launching ECS task for submissionId {}: {}", submissionId, e.getMessage(), e);
            throw new RuntimeException("Failed to launch ECS task for output generation", e);
        }
    }

    /**
     * Parses jdbc:mariadb://host:port/databaseName to extract host, port and database name.
     */
    private static DbEnv parseJdbcUrl(String url) {
        if (url == null || url.isBlank()) return null;
        Matcher m = JDBC_URL_PATTERN.matcher(url.trim());
        if (!m.find()) return null;
        String host = m.group(1);
        int port = m.group(2) != null ? Integer.parseInt(m.group(2)) : 3306;
        String databaseName = m.group(3);
        return new DbEnv(host, port, databaseName);
    }

    private record DbEnv(String host, int port, String databaseName) {}
}
