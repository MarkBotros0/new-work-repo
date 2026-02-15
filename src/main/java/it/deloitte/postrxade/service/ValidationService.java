package it.deloitte.postrxade.service;

import it.deloitte.postrxade.dto.DataQualityIssueDTO;
import it.deloitte.postrxade.dto.ExcelExportDTO;
import it.deloitte.postrxade.dto.ValidationPageDTO;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import org.springframework.scheduling.annotation.Async;

import java.util.List;


/**
 * Service interface for managing Validation workflows and Reporting.
 * <p>
 * This service handles:
 * <ul>
 * <li>Retrieving validation summaries and specific error details.</li>
 * <li>Managing the lifecycle of asynchronous report generation jobs.</li>
 * </ul>
 */
public interface ValidationService {


    /**
     * Retrieves the validation summary for submissions based on the Fiscal Year and Period.
     *
     * @param fy     The Fiscal Year.
     * @param period The specific Period.
     * @return A {@link ValidationPageDTO} containing summary statistics.
     * @throws NotFoundRecordException If the record is not found.
     */
    ValidationPageDTO getValidationSummary(Integer fy, String period) throws NotFoundRecordException;

    /**
     * Retrieves specific data quality issues (errors) based on the criteria provided.
     *
     * @param fy       The Fiscal Year.
     * @param period   The specific Period.
     * @param category The type of issue to retrieve (e.g., "error", "warning").
     * @return A list of {@link DataQualityIssueDTO}.
     * @throws NotFoundRecordException If the record is not found.
     */
    List<DataQualityIssueDTO> getDataQualityIssueDTOlIST(Integer fy, String period, String category, String type) throws NotFoundRecordException;

    /**
     * Starts the report generation job and returns the Job ID.
     * <p>
     * This acts as the trigger for the async process.
     *
     * @param fy     The Fiscal Year.
     * @param period The specific Period.
     * @return The UUID string of the started job.
     */
    String startReportJob(Integer fy, String period);

    /**
     * Checks the current status of a job.
     *
     * @param jobId The UUID string of the job.
     * @return The status string (ACCEPTED, PROCESSING, COMPLETED, FAILED, NOT_FOUND).
     */
    String getJobStatus(String jobId);

    /**
     * Retrieves the final result, handles audit logging, and cleans up the job.
     *
     * @param jobId The UUID string of the job.
     * @return The final {@link ExcelExportDTO} report.
     * @throws IllegalStateException if the job is not completed.
     */
    ExcelExportDTO getJobResult(String jobId);

    /**
     * Asynchronous worker method.
     * Kept in the interface to allow for Async proxying if needed.
     *
     * @param jobId  The job ID to update.
     * @param fy     The Fiscal Year.
     * @param period The specific Period.
     */
    @Async
    void generateExcelDataAsync(String jobId, Integer fy, String period);
}
