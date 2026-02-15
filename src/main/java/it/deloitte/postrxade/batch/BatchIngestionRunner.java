package it.deloitte.postrxade.batch;

import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.service.ObligationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * CommandLineRunner for ECS Fargate batch ingestion.
 * 
 * This component runs automatically when the application starts with the "batch" profile.
 * It executes the ingestion process and then exits with appropriate exit code.
 * 
 * Usage:
 * - Set SPRING_PROFILES_ACTIVE=batch in ECS Task Definition
 * - Container will start, run ingestion, and exit
 * - Exit code 0 = success, 1 = failure
 * 
 * Environment variables expected:
 * - DB_HOST, DB_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD (database connection)
 * - S3_BUCKET_NAME, S3_BUCKET_REGION, etc. (S3 configuration)
 */
@Component
@Profile("batch")
@Slf4j
public class BatchIngestionRunner implements CommandLineRunner {

    @Autowired
    private ObligationService obligationService;

    @Override
    public void run(String... args) throws Exception {
        Instant startTime = Instant.now();
        
        log.info("==========================================================");
        log.info("   ECS BATCH INGESTION STARTING");
        log.info("==========================================================");
        log.info("Start time: {}", startTime);
        
        try {
            // Execute the main ingestion logic
            log.info("Starting file ingestion from S3...");
            obligationService.ingestObligationFilesForMerchants();
            
            Duration duration = Duration.between(startTime, Instant.now());
            
            log.info("==========================================================");
            log.info("   ECS BATCH INGESTION COMPLETED SUCCESSFULLY");
            log.info("==========================================================");
            log.info("Total duration: {} minutes {} seconds", 
                    duration.toMinutes(), 
                    duration.toSecondsPart());
            log.info("End time: {}", Instant.now());
            
            // Exit with success code
            System.exit(0);
            
        } catch (NotFoundRecordException e) {
            Duration duration = Duration.between(startTime, Instant.now());
            
            log.error("==========================================================");
            log.error("   ECS BATCH INGESTION FAILED - Record Not Found");
            log.error("==========================================================");
            log.error("Error: {}", e.getMessage(), e);
            log.error("Duration before failure: {} minutes {} seconds", 
                    duration.toMinutes(), 
                    duration.toSecondsPart());
            
            // Exit with failure code
            System.exit(1);
            
        } catch (Exception e) {
            Duration duration = Duration.between(startTime, Instant.now());
            
            log.error("==========================================================");
            log.error("   ECS BATCH INGESTION FAILED - Unexpected Error");
            log.error("==========================================================");
            log.error("Error: {}", e.getMessage(), e);
            log.error("Duration before failure: {} minutes {} seconds", 
                    duration.toMinutes(), 
                    duration.toSecondsPart());
            
            // Exit with failure code
            System.exit(1);
        }
    }
}
