package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.dto.*;
import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.enums.IngestionTypeEnum;
import it.deloitte.postrxade.enums.SeverityEnum;
import it.deloitte.postrxade.enums.SubmissionStatusEnum;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.repository.*;
import it.deloitte.postrxade.service.*;
import it.deloitte.postrxade.utils.AuditLogger;
import it.deloitte.postrxade.utils.ReportJob;
import lombok.extern.slf4j.Slf4j;
import ma.glasnost.orika.MapperFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


/**
 * Service Implementation for managing validation logic, data aggregation, and report generation.
 * <p>
 * This service acts as the bridge between the Controller and the repositories. It handles:
 * <ul>
 * <li>Retrieving and validating Fiscal Years and Periods.</li>
 * <li>Aggregating error and warning statistics for the dashboard.</li>
 * <li>Filtering specific error rows for detail views.</li>
 * <li>Asynchronously generating Excel exports to avoid HTTP timeouts.</li>
 * </ul>
 */
@Service
@Slf4j
public class ValidationServiceImpl implements ValidationService {
    @Autowired
    private SubmissionService submissionService;

    @Autowired
    private ErrorRecordRepository errorRecordRepository;

    @Autowired
    private ErrorCauseRepository errorCauseRepository;

    @Autowired
    private ErrorTypeRepository errorTypeRepository;

    @Autowired
    private ErrorTypeService errorTypeService;

    @Autowired
    private PeriodRepository periodRepository;

    @Autowired
    private ObligationRepository obligationRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private SoggettiRepository soggettiRepository;

    @Autowired
    private RapportiRepository rapportiRepository;

    @Autowired
    private DatiContabiliRepository datiContabiliRepository;

    @Autowired
    private CollegamentiRepository collegamentiRepository;

    @Autowired
    private IngestionRepository ingestionRepository;

    @Autowired
    private ObligationService obligationService;

    @Autowired
    private AuditLogger appLogger;

    @Autowired
    private UserService userService;

    @Autowired
    @Qualifier("alternativeMapperFacade")
    private MapperFacade alternativeMapperFacade;

    /**
     * In-memory storage for tracking asynchronous report jobs.
     * Uses ConcurrentHashMap to ensure thread safety during async access.
     */
    public static final Map<String, ReportJob> jobStorage = new ConcurrentHashMap<>();

    /**
     * Retrieves the validation summary (counts of errors/warnings) for a specific FY and Period.
     * <p>
     * Logic Flow:
     * 1. Validate existence of Period and Obligation (Return empty summary if missing).
     * 2. Delegate validity checks to {@link ObligationService} (Throws exception if multiple active submissions exist).
     * 3. Fetch the active submission.
     * 4. Aggregate statistics.
     *
     * @param fy         The Fiscal Year.
     * @param periodName The month name (e.g., "January").
     * @return A {@link ValidationPageDTO} containing the calculated statistics.
     * @throws NotFoundRecordException if the obligation state is invalid (e.g., multiple active submissions).
     */
    @Override
    @Transactional(readOnly = true)
    public ValidationPageDTO getValidationSummary(Integer fy, String periodName) throws NotFoundRecordException {

        Obligation obligation;

        Period period = periodRepository.findByName(periodName).orElse(null);

        obligation = obligationRepository.findByFiscalYearAndPeriod(fy, period).orElseThrow(
                () -> new NotFoundRecordException("No Obligation found for this period"));

        return buildValidationFromSubmissions(obligation);
    }


    /**
     * Retrieves a detailed list of specific data quality issues (Errors or Warnings).
     *
     * @param fy       The Fiscal Year.
     * @param period   The month name.
     * @param category The filter type ("errors" or other).
     * @return A list of {@link DataQualityIssueDTO}.
     * @throws NotFoundRecordException if underlying data retrieval fails.
     */
    @Override
    @Transactional(readOnly = true)
    public List<DataQualityIssueDTO> getDataQualityIssueDTOlIST(
            Integer fy, String period, String category, String type) throws NotFoundRecordException {

        List<Submission> submissions = obligationService.getAllSubmissionByFyAndPeriod(fy, period);
        Submission activeSubmission = submissions.stream()
                .filter(s -> {
                            return !s.getCurrentSubmissionStatus().getName().equals(SubmissionStatusEnum.ERROR.getDbName())
                                    && !s.getCurrentSubmissionStatus().getName().equals(SubmissionStatusEnum.CANCELLED.getDbName())
                                    && !s.getCurrentSubmissionStatus().getName().equals(SubmissionStatusEnum.REJECTED.getDbName());
                        }
                ).findFirst().orElse(null);

        List<DataQualityIssueDTO> resultList = new ArrayList<>();

        if (activeSubmission == null) return resultList;

        List<ErrorType> errorTypes;
        if (type.equals("ALL")) {
            errorTypes = errorTypeRepository.findBySubmissionIdAndSeverity(
                    activeSubmission.getId(), Integer.parseInt(category));
        } else {
            ErrorType errorType = errorTypeRepository.findByName(type).orElse(null);
            if (errorType == null) return Collections.emptyList();
            errorTypes = List.of(errorType);
        }

        for (ErrorType errorType : errorTypes) {
            List<ErrorRecord> errors = errorRecordRepository.findRecordsByErrorCodeAndSubmissionId(
                    activeSubmission.getId(), errorType.getErrorCode());

            int errorCauses = errorCauseRepository.findBySubmissionIdAndErrorCode(
                    activeSubmission.getId(), errorType.getErrorCode());

            DataQualityIssueDTO dto = DataQualityIssueDTO.builder()
                    .errorLevel(errorType.getSeverityLevel() == 1 ? "Warning" : "Error")
                    .errorCode(errorType.getErrorCode())
                    .errorName(errorType.getName())
                    .description(errorType.getDescription())
                    .errorsCount(errorCauses)
                    .examples(errors.stream().map(ErrorRecord::getRawRow).limit(5).toList())
                    .build();

            resultList.add(dto);
        }
        return resultList;
    }

// ==================================================================================
// ASYNC REPORT MANAGEMENT
// ==================================================================================

    /**
     * Initializes the report job and starts the async process.
     *
     * @return The Job ID.
     */
    public String startReportJob(Integer fy, String period) {
        String jobId = UUID.randomUUID().toString();

        ReportJob job = ReportJob.builder()
                .jobId(jobId)
                .status("ACCEPTED")
                .build();

        jobStorage.put(jobId, job);

        // Trigger the actual async processing
        generateExcelDataAsync(jobId, fy, period);

        return jobId;
    }

    /**
     * Checks the status of a job.
     */
    public String getJobStatus(String jobId) {
        ReportJob job = jobStorage.get(jobId);
        return (job != null) ? job.getStatus() : "NOT_FOUND";
    }

    /**
     * Retrieves the result, logs the action, and cleans up the job.
     */
    public ExcelExportDTO getJobResult(String jobId) {
        ReportJob job = jobStorage.get(jobId);

        if (job == null || !"COMPLETED".equals(job.getStatus())) {
            throw new IllegalStateException("Job not found or not completed");
        }

        // Clean up memory
        jobStorage.remove(jobId);

        // Perform Audit Logging
        logReportDownload();

        return job.getResult();
    }

    private void logReportDownload() {
        try {
            appLogger.save(Log.builder()
                    .message(AuditLogger.DOWNLOAD_REPORT) // Assuming static constant access
                    .timestamp(Instant.now())
                    .updater(alternativeMapperFacade.map(userService.getCurrentUser(), User.class))
                    .build());
        } catch (Exception e) {
            // Log without user details if user validation fails
            appLogger.save(Log.builder()
                    .message(AuditLogger.DOWNLOAD_REPORT)
                    .timestamp(Instant.now())
                    .build());
        }
    }

    /**
     * Asynchronously generates the Excel report data.
     * <p>
     * This method is annotated with {@link Async} to run on a separate thread.
     * It updates the {@link ReportJob} status in {@link #jobStorage} as it progresses.
     *
     * @param jobId  The unique ID of the job to update.
     * @param fy     The Fiscal Year.
     * @param period The month name.
     */
    @Async
    public void generateExcelDataAsync(String jobId, Integer fy, String period) {
        try {
            // 1. Update status to processing
            ReportJob job = jobStorage.get(jobId);
            if (job == null) return; // Safety check
            job.setStatus("PROCESSING");

            // 2. Fetch Data
            Optional<Submission> submission = obligationService.getActiveSubmissionForStats(fy, period);
            List<ExcelRowDTO> errors = new ArrayList<>();
            List<ExcelRowDTO> warnings = new ArrayList<>();

            if (submission.isPresent()) {
                List<Ingestion> ingestions = ingestionRepository.findBySubmission_Id(submission.get().getId());
                List<ErrorCause> causes = errorCauseRepository.findBySubmissionId(submission.get().getId());

                for (Ingestion ingestion : ingestions) {
                    List<ExcelRowDTO> rows = causes.stream().map(c -> mapToExcelRow(c, submission.get(), ingestion)).toList();
                    errors.addAll(rows.stream().filter(r -> r.getType().equals("error")).toList());
                    warnings.addAll(rows.stream().filter(r -> r.getType().equals("warning")).toList());
                }
            }

            ExcelExportDTO result = ExcelExportDTO.builder()
                    .errors(errors)
                    .warnings(warnings)
                    .build();

            job.setResult(result);
            job.setStatus("COMPLETED");

        } catch (Exception e) {
            ReportJob job = jobStorage.get(jobId);
            if (job != null) {
                job.setStatus("FAILED");
                job.setErrorMessage(e.getMessage());
            }
        }
    }

// ==================================================================================
// PRIVATE HELPER METHODS
// ==================================================================================

    /**
     * Maps an ErrorRecord entity to an ExcelRowDTO for reporting.
     */
    private ExcelRowDTO mapToExcelRow(ErrorCause errorCause, Submission submission, Ingestion ingestion) {
        String type = errorCause.getErrorType().getSeverityLevel() == 1 ? "warning" : "error";
        return ExcelRowDTO.builder()
                .rawRecord(errorCause.getErrorRecord().getRawRow())
                .type(type)
                .name(errorCause.getErrorType().getName())
                .description(errorCause.getErrorType().getDescription())
                .batchId(submission.getBatchId())
                .timestamp(String.valueOf(ingestion.getIngestedAt()))
                .fileType(ingestion.getIngestionType().getName())
                .build();
    }

    /**
     * Aggregates validation statistics from a list of submissions.
     * OPTIMIZED: Uses native SQL aggregation queries instead of loading all ErrorCause entities into memory.
     * This prevents OOM and timeout issues with millions of records.
     */
    private ValidationPageDTO buildValidationFromSubmissions(Obligation obligation) throws NotFoundRecordException {
        long methodStartTime = System.currentTimeMillis();
        Submission submission = obligationService.checkIfValid(obligation);
        Long submissionId = submission.getId();

        log.info("Starting buildValidationFromSubmissions for submissionId={}", submissionId);

        // 1. Aggregated Data Fetching (Reduce DB Roundtrips)
        // You should create a projection or DTO to hold counts grouped by Type and Severity
        Map<String, Long> totalsMap = new HashMap<>();
        totalsMap.put(IngestionTypeEnum.SOGGETTI.getLabel(), soggettiRepository.countBySubmissionId(submissionId));
        totalsMap.put(IngestionTypeEnum.RAPPORTI.getLabel(), rapportiRepository.countBySubmissionId(submissionId));
        totalsMap.put(IngestionTypeEnum.DATI_CONTABILI.getLabel(), datiContabiliRepository.countBySubmissionId(submissionId));
        totalsMap.put(IngestionTypeEnum.COLLEGAMENTI.getLabel(), collegamentiRepository.countBySubmissionId(submissionId));

        long totalTransactions = totalsMap.values().stream().mapToLong(Long::longValue).sum();
        long totalErrorRecords = errorRecordRepository.countBySubmissionId(submissionId);

        // 2. Build Issues Groups using a helper to avoid boilerplate
        IssuesGroup soggettiIssues = buildIssuesGroup(submissionId, IngestionTypeEnum.SOGGETTI);
        IssuesGroup rapportiIssues = buildIssuesGroup(submissionId, IngestionTypeEnum.RAPPORTI);
        IssuesGroup datiContabiliIssues = buildIssuesGroup(submissionId, IngestionTypeEnum.DATI_CONTABILI);
        IssuesGroup collegamentiIssues = buildIssuesGroup(submissionId, IngestionTypeEnum.COLLEGAMENTI);

        log.info("buildValidationFromSubmissions completed in {}ms for submissionId={}",
                System.currentTimeMillis() - methodStartTime, submissionId);

        return new ValidationPageDTO(
                totalTransactions,
                totalErrorRecords,
                rapportiIssues,
                soggettiIssues,
                datiContabiliIssues,
                collegamentiIssues
        );
    }

    /**
     * Helper to consolidate the logic for each Ingestion Type
     */
    private IssuesGroup buildIssuesGroup(Long submissionId, IngestionTypeEnum type) {
        String typeLabel = type.getLabel();

        return new IssuesGroup(
                getValidationGroup(submissionId, typeLabel, SeverityEnum.ERROR),
                getValidationGroup(submissionId, typeLabel, SeverityEnum.WARNING)
        );
    }

    private ValidationGroup getValidationGroup(Long submissionId, String typeLabel, SeverityEnum severity) {
        // Total count for this specific group
        long total = errorCauseRepository.countDistinctErrorRecordsBySubmissionIdAndSeverityAndIngestionType(
                submissionId, severity.getLevel(), typeLabel);

        // Specific reasons
        List<ValidationReason> reasons = errorCauseRepository
                .findErrorTypeCountsBySubmissionIdAndSeverityAndIngestionType(submissionId, severity.getLevel(), typeLabel)
                .stream()
                .map(dto -> new ValidationReason(dto.errorTypeName(), dto.errorCode(), dto.count()))
                .collect(Collectors.toList());

        return new ValidationGroup(total, reasons);
    }
}
