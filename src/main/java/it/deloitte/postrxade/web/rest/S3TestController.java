package it.deloitte.postrxade.web.rest;

import it.deloitte.postrxade.service.S3Service;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test controller for S3 connectivity verification.
 */
@RestController
@RequestMapping("/api/s3-test")
@CrossOrigin(origins = {"http://localhost:8082", "http://localhost:8080", "http://localhost:3000", "http://localhost:4200"})
@Tag(name = "S3 Test", description = "Test S3 connectivity and operations")
public class S3TestController {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3TestController.class);

    @Autowired
    private S3Service s3Service;

    /**
     * Tests S3 connectivity by listing objects.
     */
    @GetMapping("/connectivity")
    @Operation(summary = "Test S3 connectivity", description = "Tests S3 connection by listing objects")
    public ResponseEntity<Map<String, Object>> testConnectivity() {
        try {
            LOGGER.info("Testing S3 connectivity...");
            
            // Test basic connectivity by listing objects
            List<S3Object> objects = s3Service.listObjects("");
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "S3 connection successful!");
            response.put("bucketName", s3Service.getBucketName());
            response.put("objectCount", objects.size());
            response.put("objects", objects);
            
            LOGGER.info("S3 connectivity test successful. Found {} objects", objects.size());
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            LOGGER.error("S3 connectivity test failed: {}", e.getMessage(), e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "S3 connection failed: " + e.getMessage());
            response.put("error", e.getClass().getSimpleName());
            
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Tests S3 bucket access by checking if bucket exists.
     */
    @GetMapping("/bucket-info")
    @Operation(summary = "Get bucket info", description = "Gets information about the S3 bucket")
    public ResponseEntity<Map<String, Object>> getBucketInfo() {
        try {
            LOGGER.info("Getting S3 bucket information...");
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("bucketName", s3Service.getBucketName());
            response.put("message", "Bucket accessible via IAM Role");
            
            LOGGER.info("Bucket info retrieved successfully");
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            LOGGER.error("Failed to get bucket info: {}", e.getMessage(), e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to access bucket: " + e.getMessage());
            
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Tests file upload to S3.
     */
    @PostMapping("/test-upload")
    @Operation(summary = "Test file upload", description = "Tests uploading a test file to S3")
    public ResponseEntity<Map<String, Object>> testUpload() {
        try {
            LOGGER.info("Testing S3 file upload...");
            
            // Create a test file content
            String testContent = "Test file created at: " + java.time.LocalDateTime.now();
            String testKey = "test-connectivity/" + System.currentTimeMillis() + ".txt";
            
            // Upload test file
            String uploadedKey = s3Service.uploadFile(
                testKey, 
                new java.io.ByteArrayInputStream(testContent.getBytes()), 
                "text/plain"
            );
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "File uploaded successfully!");
            response.put("uploadedKey", uploadedKey);
            response.put("bucketName", s3Service.getBucketName());
            
            LOGGER.info("Test file uploaded successfully: {}", uploadedKey);
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            LOGGER.error("Test upload failed: {}", e.getMessage(), e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Upload failed: " + e.getMessage());
            
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Tests file upload to S3 OUTPUT folder for homepage testing.
     */
    @GetMapping("/test-output-upload")
    @Operation(summary = "Test OUTPUT folder upload", description = "Tests uploading a test file to S3 OUTPUT folder")
    public ResponseEntity<Map<String, Object>> testOutputUpload() {
        try {
            LOGGER.info("Testing S3 file upload to OUTPUT folder...");
            
            // Create a test file content
            String testContent = "Test file per connessione S3\n" +
                               "Creato il: " + java.time.LocalDateTime.now() + "\n" +
                               "Sistema: POS Transaction ADE Backend\n" +
                               "Tipo: Test di connettività S3\n" +
                               "Cartella: OUTPUT\n" +
                               "Status: SUCCESS";
            
            String testKey = "OUTPUT/test-connectivity-" + System.currentTimeMillis() + ".txt";
            
            // Upload test file to OUTPUT folder
            String uploadedKey = s3Service.uploadFile(
                testKey, 
                new java.io.ByteArrayInputStream(testContent.getBytes()), 
                "text/plain"
            );
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "File caricato con successo nella cartella OUTPUT!");
            response.put("uploadedKey", uploadedKey);
            response.put("bucketName", s3Service.getBucketName());
            response.put("folder", "OUTPUT");
            response.put("timestamp", java.time.LocalDateTime.now().toString());
            
            LOGGER.info("Test file uploaded successfully to OUTPUT folder: {}", uploadedKey);
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            LOGGER.error("Test upload to OUTPUT folder failed: {}", e.getMessage(), e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Upload fallito: " + e.getMessage());
            response.put("error", e.getClass().getSimpleName());
            response.put("timestamp", java.time.LocalDateTime.now().toString());
            
            return ResponseEntity.badRequest().body(response);
        }
    }
}
