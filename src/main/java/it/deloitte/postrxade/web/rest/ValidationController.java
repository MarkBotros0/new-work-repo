package it.deloitte.postrxade.web.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import it.deloitte.postrxade.dto.DataQualityIssueDTO;
import it.deloitte.postrxade.dto.ExcelExportDTO;
import it.deloitte.postrxade.dto.ValidationPageDTO;
import it.deloitte.postrxade.enums.AuthIdProfilo;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.security.RequireAuthorities;
import it.deloitte.postrxade.service.UserService;
import it.deloitte.postrxade.service.ValidationService;
import it.deloitte.postrxade.utils.AuditLogger;
import ma.glasnost.orika.MapperFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;


/**
 * REST controller for managing validation data, error retrieval, and asynchronous report generation.
 * <p>
 * This controller provides endpoints to:
 * <ul>
 * <li>Retrieve validation summaries based on Fiscal Year (FY) and Period.</li>
 * <li>Fetch specific data quality issues/errors.</li>
 * <li>Handle the asynchronous generation of Excel reports (Start Job, Check Status, Get Result).</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/validations")
@CrossOrigin("*")
@Tag(name = "Validation Reporting Service", description = "The main endpoint for Validation and Reporting")
public class ValidationController {

    private final ValidationService validationService;

    @Autowired
    private @Qualifier("alternativeMapperFacade") MapperFacade alternativeMapperFacade;

    public ValidationController(AuditLogger appLogger, ValidationService validationService, UserService userService) {
        this.validationService = validationService;
    }


    /**
     * Retrieves the validation summary for submissions based on the Fiscal Year and Period.
     * <p>
     * Endpoint: GET /api/validations/submissions/validation
     *
     * @param fy     The Fiscal Year (e.g., 2024).
     * @param period The specific month name (e.g. "January" ).
     * @return ResponseEntity containing the {@link ValidationPageDTO} with summary data.
     * @throws NotFoundRecordException if the obligation state is invalid (e.g., multiple active submissions).
     */
    @GetMapping("/submissions/validation")
    @Operation(summary = "Get submission validation summary", description = "Request to get validation summary by given FY and Period")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<ValidationPageDTO> getObligationValidation(
            @RequestParam("fy") Integer fy,
            @RequestParam("period") String period) throws NotFoundRecordException {
        ValidationPageDTO validationData = validationService.getValidationSummary(fy, period);
        return new ResponseEntity<>(validationData, HttpStatus.OK);
    }

    /**
     * Retrieves specific data quality issues (errors) based on the criteria provided.
     * <p>
     * Endpoint: GET /api/validations/errors
     *
     * @param fy     The Fiscal Year.
     * @param period The specific month name (e.g. "January" ).
     * @param type   The type of error/issue to filter by.
     * @return A list of {@link DataQualityIssueDTO} objects representing the errors found.
     * @throws NotFoundRecordException if the obligation state is invalid (e.g., multiple active submissions).
     */
    @GetMapping("/issues")
    @Operation(summary = "Get obligation validation summary", description = "Request to get obligation error rows by given FY and Period")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public List<DataQualityIssueDTO> getErrors(
            @RequestParam("fiscalYear") Integer fy,
            @RequestParam("period") String period,
            @RequestParam("category") String category,
            @RequestParam(value = "type", defaultValue = "ALL") String type
    ) throws NotFoundRecordException {
        return validationService.getDataQualityIssueDTOlIST(fy, period, category, type);
    }

    /**
     * Initiates the asynchronous report generation process.
     * <p>
     * Endpoint: POST /api/validations/download/start
     * <p>
     * Logic moved to Service: {@link ValidationService#startReportJob(Integer, String)} handles
     * ID generation, job creation, and starting the async process.
     *
     * @param fy     The Fiscal Year for the report.
     * @param period The specific month name for the report (e.g. "January").
     * @return A Map containing the generated "jobId".
     */
    @PostMapping("/download/start")
    @Operation(summary = "Start report generation", description = "Returns a Job ID to track progress")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<Map<String, String>> startReportGeneration(
            @RequestParam("fy") Integer fy,
            @RequestParam("period") String period) {

        String jobId = validationService.startReportJob(fy, period);

        return ResponseEntity.ok(Map.of("jobId", jobId));
    }

    /**
     * Polls the status of a specific report generation job.
     * <p>
     * Endpoint: GET /api/validations/download/status/{jobId}
     *
     * @param jobId The UUID string of the job returned by the start endpoint.
     * @return A Map containing the current "status" of the job (e.g., ACCEPTED, PROCESSING, COMPLETED).
     * Returns 404 NOT_FOUND if the jobId does not exist.
     */
    @GetMapping("/download/status/{jobId}")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<Map<String, String>> getJobStatus(@PathVariable String jobId) {

        String status = validationService.getJobStatus(jobId);

        if ("NOT_FOUND".equals(status)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("status", "NOT_FOUND"));
        }

        return ResponseEntity.ok(Map.of("status", status));
    }

    /**
     * Retrieves the final result of the report generation job.
     * <p>
     * The Service layer now handles:
     * 1. Checking if the job is COMPLETED.
     * 2. Removing the job from memory.
     * 3. Logging the audit event.
     */
    @GetMapping("/download/result/{jobId}")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<ExcelExportDTO> getJobResult(@PathVariable String jobId) {
        try {
            ExcelExportDTO result = validationService.getJobResult(jobId);
            return ResponseEntity.ok(result);
        } catch (IllegalStateException e) {
            // Service throws IllegalStateException if job is not found or not completed
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }
}
