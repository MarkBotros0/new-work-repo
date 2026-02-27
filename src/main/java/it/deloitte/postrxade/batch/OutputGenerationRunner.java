package it.deloitte.postrxade.batch;

import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.service.OutputService;
import it.deloitte.postrxade.tenant.TenantContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * CommandLineRunner for ECS Fargate output generation task.
 *
 * Multi-tenant: il tenant per questo run è passato da chi lancia il task (es. backend in contesto nexi
 * passa TENANT_ID=nexi e le variabili DB/S3 per nexi). L'output finisce nella cartella S3 del tenant.
 *
 * Usage:
 * - Set SPRING_PROFILES_ACTIVE=output in ECS Task Definition
 * - Chi lancia il task (EcsTaskService) passa TENANT_ID, SUBMISSION_ID, DB_* e S3_BUCKET_* nel Container Override
 * - Exit code 0 = success, 1 = failure
 *
 * Environment variables expected:
 * - TENANT_ID: tenant per questo run (nexi o amex), obbligatorio
 * - SUBMISSION_ID: The submission ID to generate output for
 * - DB_HOST, DB_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD (database del tenant)
 * - S3_BUCKET_NAME, S3_BUCKET_REGION, S3_BUCKET_OUTPUT_FOLDER (cartella output per tenant, es. NEXI/OUTPUT, AMEX/OUTPUT)
 */
@Component
@Profile("output")
@Slf4j
public class OutputGenerationRunner implements CommandLineRunner {

    @Autowired
    private OutputService outputService;

    @Value("${TENANT_ID:}")
    private String tenantIdEnv;

    @Value("${aws.s3.output-folder:}")
    private String s3OutputFolder;

    @Override
    public void run(String... args) throws Exception {
        Instant startTime = Instant.now();

        // Multi-tenant: imposta il tenant per questo run (chi lancia il task passa TENANT_ID)
        String effectiveTenant = tenantIdEnv != null && !tenantIdEnv.isBlank() ? tenantIdEnv.trim() : null;
        if (effectiveTenant == null || effectiveTenant.isEmpty()) {
            log.error("TENANT_ID environment variable is not set. Output generation must run in tenant context (nexi or amex).");
            System.exit(1);
            return;
        }
        TenantContext.setTenantId(effectiveTenant);
        log.info("Output run tenant: {}", effectiveTenant);
        // Log della cartella S3: se vedi "OUTPUT" invece di "NEXI/OUTPUT" o "AMEX/OUTPUT", gli override del backend non sono stati applicati (nome container nella task definition diverso da aws.ecs.container-name-output)
        log.info("S3 output folder from env: [{}] (expected: {}/OUTPUT)", s3OutputFolder != null ? s3OutputFolder : "(empty)", effectiveTenant.toUpperCase());

        // Get submission ID from environment variable
        String submissionIdStr = System.getenv("SUBMISSION_ID");
        if (submissionIdStr == null || submissionIdStr.isEmpty()) {
            log.error("SUBMISSION_ID environment variable is not set");
            System.exit(1);
            return;
        }

        Long submissionId;
        try {
            submissionId = Long.parseLong(submissionIdStr);
        } catch (NumberFormatException e) {
            log.error("Invalid SUBMISSION_ID format: {}", submissionIdStr);
            System.exit(1);
            return;
        }
        
        log.info("==========================================================");
        log.info("   ECS OUTPUT GENERATION STARTING");
        log.info("==========================================================");
        log.info("Submission ID: {}", submissionId);
        log.info("Start time: {}", startTime);
        
        try {
            // Execute the output generation
            log.info("Starting output generation for submissionId={}...", submissionId);
            outputService.generateSubmissionOutputTxt(submissionId);
            
            Duration duration = Duration.between(startTime, Instant.now());
            
            log.info("==========================================================");
            log.info("   ECS OUTPUT GENERATION COMPLETED SUCCESSFULLY");
            log.info("==========================================================");
            log.info("Submission ID: {}", submissionId);
            log.info("Total duration: {} minutes {} seconds", 
                    duration.toMinutes(), 
                    duration.toSecondsPart());
            log.info("End time: {}", Instant.now());
            
            // Exit with success code
            System.exit(0);
            
        } catch (NotFoundRecordException e) {
            Duration duration = Duration.between(startTime, Instant.now());
            
            log.error("==========================================================");
            log.error("   ECS OUTPUT GENERATION FAILED - Record Not Found");
            log.error("==========================================================");
            log.error("Submission ID: {}", submissionId);
            log.error("Error: {}", e.getMessage());
            log.error("Total duration: {} minutes {} seconds", 
                    duration.toMinutes(), 
                    duration.toSecondsPart());
            
            // Exit with error code
            System.exit(1);
            
        } catch (IOException e) {
            Duration duration = Duration.between(startTime, Instant.now());
            
            log.error("==========================================================");
            log.error("   ECS OUTPUT GENERATION FAILED - IO Error");
            log.error("==========================================================");
            log.error("Submission ID: {}", submissionId);
            log.error("Error: {}", e.getMessage(), e);
            log.error("Total duration: {} minutes {} seconds", 
                    duration.toMinutes(), 
                    duration.toSecondsPart());
            
            // Exit with error code
            System.exit(1);
            
        } catch (Exception e) {
            Duration duration = Duration.between(startTime, Instant.now());
            
            log.error("==========================================================");
            log.error("   ECS OUTPUT GENERATION FAILED - Unexpected Error");
            log.error("==========================================================");
            log.error("Submission ID: {}", submissionId);
            log.error("Error: {}", e.getMessage(), e);
            log.error("Total duration: {} minutes {} seconds", 
                    duration.toMinutes(), 
                    duration.toSecondsPart());
            
            // Exit with error code
            System.exit(1);
        }
    }
}
