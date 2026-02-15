package it.deloitte.postrxade.utils;

import it.deloitte.postrxade.dto.ExcelExportDTO;
import it.deloitte.postrxade.service.impl.ValidationServiceImpl;
import lombok.Builder;
import lombok.Data;

/**
 * Model class representing the state of an asynchronous report generation job.
 * <p>
 * This class is stored in the {@link ValidationServiceImpl#jobStorage} map to track
 * the progress of long-running tasks. It holds the status, the final result (once ready),
 * or error details if the job failed.
 */
@Builder
@Data
public class ReportJob {

    /**
     * Unique identifier for the job (UUID).
     */
    private String jobId;

    /**
     * Current status of the job.
     * Common values: "ACCEPTED", "PROCESSING", "COMPLETED", "FAILED", "NOT_FOUND".
     */
    private String status;

    /**
     * The generated report data.
     * This field is null until the status is "COMPLETED".
     */
    private ExcelExportDTO result;

    /**
     * Error message if the job failed.
     * This field is populated only when status is "FAILED".
     */
    private String errorMessage;
}
