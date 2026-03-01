package it.deloitte.postrxade.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.enums.*;
import it.deloitte.postrxade.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import it.deloitte.postrxade.dto.PeriodSubmissionData;
import it.deloitte.postrxade.dto.SubmissionCustomDTO;
import it.deloitte.postrxade.dto.ValidationDTO;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.parser.merchants.MerchantFileProcessingService;
import it.deloitte.postrxade.parser.transaction.RemoteFile;
import it.deloitte.postrxade.records.FileDescriptor;
import it.deloitte.postrxade.records.ProcessedRecordBatch;
import it.deloitte.postrxade.records.StagingResult;
import it.deloitte.postrxade.service.IngestionService;
import it.deloitte.postrxade.service.ObligationService;
import it.deloitte.postrxade.service.S3Service;
import it.deloitte.postrxade.service.StagingIngestionService;
import it.deloitte.postrxade.service.SubmissionService;
import lombok.extern.slf4j.Slf4j;
import ma.glasnost.orika.MapperFacade;
import software.amazon.awssdk.services.s3.model.S3Object;


/**
 * Implementation of the ObligationService.
 * <p>
 * This service manages the lifecycle and retrieval of Obligations and their Submissions.
 * It enforces strict data integrity rules (e.g., ensuring only one active submission exists per obligation)
 * and provides helper methods for historical data retrieval.
 */
@Service
@Slf4j
public class ObligationServiceImpl implements ObligationService {

    private static final String PERIOD_NOT_FOUND_MSG = "Period with name %s not found";
    private static final String OBLIGATION_NOT_FOUND_MSG = "Obligation not found for period %s and fiscal year %d";
    private static final String OBLIGATION_NOT_VALID_MSG = "Obligation with period %s and fiscal year %s has invalid number of active submission.";
    private static final String ERROR_TAG = "error";
    private static final String WARNING_TAG = "warning";

    @Autowired
    private ObligationRepository obligationRepository;

    @Autowired
    private PeriodRepository periodRepository;

    @Autowired
    private SubmissionService submissionService;

    @Autowired
    private IngestionService ingestionService;

    @Autowired
    private IngestionTypeRepository ingestionTypeRepository;
    @Autowired
    private SoggettiRepository soggettiRepository;
    @Autowired
    private RapportiRepository rapportiRepository;
    @Autowired
    private DatiContabiliRepository datiContabiliRepository;
    @Autowired
    private CambioNdgRepository cambioNdgRepository;
    @Autowired
    private CollegamentiRepository collegamentiRepository;

    @Autowired
    private IngestionErrorRepository ingestionErrorRepository;

    @Autowired
    private IngestionStatusRepository ingestionStatusRepository;

    @Autowired
    private LogRepository logRepository;

    @Autowired
    private MerchantFileProcessingService merchantFileProcessingService;

    @Autowired
    private @Qualifier("alternativeMapperFacade") MapperFacade alternativeMapperFacade;

    @Autowired
    private SubmissionRepository submissionRepository;
    @Autowired
    private IngestionRepository ingestionRepository;

    @Autowired
    private OutputRepository outputRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private ResolvedTransactionRepository resolvedTransactionRepository;

    @Autowired
    private MerchantRepository merchantRepository;

    @Autowired
    private ErrorRecordRepository errorRecordRepository;

    @Autowired
    private ErrorCauseRepository errorCauseRepository;

    @Autowired
    private ErrorTypeRepository errorTypeRepository;

    @Autowired
    @Qualifier("mapperFacade")
    private MapperFacade mapperFacade;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private StagingIngestionService stagingIngestionService;

    @Value("${aws.s3.input-folder}")
    private String inputFolder;

    // Flag to enable staging-based ingestion (high-performance ETL approach)
    // Set to true for production with large files (250k+ merchants, 800k+ transactions)
    @Value("${application.ingestion.use-staging:true}")
    private boolean useStagingIngestion;

    /**
     * Retrieves all submissions for a given Fiscal Year and Period.
     * <p>
     * Performs a strict integrity check:
     * If more than one active submission exists, it throws a {@link NotFoundRecordException}.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period name.
     * @return A list of all Submissions (Active + Cancelled/Rejected).
     * @throws NotFoundRecordException if the period/obligation is missing or data integrity is violated.
     */
    @Override
    @Transactional(readOnly = true)
    public List<Submission> getAllSubmissionByFyAndPeriod(Integer fy, String period) throws NotFoundRecordException {

        Optional<Period> optionalPeriod = periodRepository.findByName(period);
        if (optionalPeriod.isEmpty()) {
            throw new NotFoundRecordException(String.format(PERIOD_NOT_FOUND_MSG, period));
        }
        Period existingPeriod = optionalPeriod.get();

        Optional<Obligation> optionalObligation = obligationRepository.findByFiscalYearAndPeriod(fy, existingPeriod);
        if (optionalObligation.isEmpty()) {
            throw new NotFoundRecordException(String.format(OBLIGATION_NOT_FOUND_MSG, period, fy));
        }

        Obligation obligation = optionalObligation.get();

        // Integrity Check: Throws 404 if >1 active submission
        checkIfValid(obligation);

        return submissionRepository.findByObligationId(obligation.getId());
    }

    /**
     * Calculates the target past period based on the offset and retrieves its submissions.
     *
     * @param fiscalYear      The starting Fiscal Year.
     * @param periodName      The starting Period name.
     * @param periodsToGoBack Number of periods to regress.
     * @return List of submissions for the calculated past period, or empty list if not found.
     */
    @Override
    @Transactional(readOnly = true)
    public List<Submission> getSubmissionsFromPastPeriod(Integer fiscalYear, String periodName, int periodsToGoBack) {
        if (periodsToGoBack <= 0) return Collections.emptyList();

        Optional<Period> optionalCurrentPeriod = periodRepository.findByName(periodName);
        if (optionalCurrentPeriod.isEmpty()) return Collections.emptyList();

        int currentOrder = optionalCurrentPeriod.get().getOrder();
        int yearsToSubtract = periodsToGoBack / 12;
        int monthsToSubtract = periodsToGoBack % 12;
        int targetFiscalYear = fiscalYear - yearsToSubtract;
        int targetPeriodOrder = currentOrder - monthsToSubtract;

        if (targetPeriodOrder <= 0) {
            targetFiscalYear -= 1;
            targetPeriodOrder += 12;
        }

        Optional<Period> optionalTargetPeriod = periodRepository.findByOrder(targetPeriodOrder);
        if (optionalTargetPeriod.isEmpty()) return Collections.emptyList();
        Period targetPeriod = optionalTargetPeriod.get();

        try {
            // We re-use the Strict method here, but CATCH the error
            return getAllSubmissionByFyAndPeriod(targetFiscalYear, targetPeriod.getName());
        } catch (NotFoundRecordException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Validates the integrity of an Obligation.
     * <p>
     * Rule: An obligation can have at most ONE active submission.
     * 0 active is Valid. 1 active is Valid. >1 is Invalid.
     *
     * @param obligation The entity to check.
     * @throws NotFoundRecordException if validation fails.
     */
    @Override
    public Submission checkIfValid(Obligation obligation) throws NotFoundRecordException {
        Submission submission = submissionService.getActivSubmission(obligation);

        if (submission == null) {
            throw new NotFoundRecordException(String.format(OBLIGATION_NOT_VALID_MSG,
                    obligation.getPeriod().getName(),
                    obligation.getFiscalYear()));

        }

        return submission;
    }

    /**
     * Retrieves the single active submission for statistics purposes.
     * <p>
     * Returns {@link Optional#empty()} if validation fails or no active submission exists,
     * rather than throwing an exception (except for strict validity checks).
     */
    @Override
    @Transactional(readOnly = true)
    public Optional<Submission>  getActiveSubmissionForStats(Integer fy, String period) throws NotFoundRecordException {

        // 1. Check Period & Obligation (Return Empty if missing)
        Optional<Period> optionalPeriod = periodRepository.findByName(period);
        if (optionalPeriod.isEmpty()) return Optional.empty();

        Optional<Obligation> optionalObligation = obligationRepository.findByFiscalYearAndPeriod(fy, optionalPeriod.get());
        if (optionalObligation.isEmpty()) return Optional.empty();

        Obligation obligation = optionalObligation.get();

        // 2. STRICT INTEGRITY CHECK (The only time we throw Error)
        checkIfValid(obligation);

        // 3. Filter for the single Active Submission
        if (obligation.getSubmissions() == null) return Optional.empty();

        return obligation.getSubmissions().stream()
                .filter(this::isActiveSubmission)
                .findFirst();
    }

    /**
     * Retrieves PeriodSubmissionData DTO for a specific Fiscal Year and Period.
     * <p>
     * Used mainly for charts or summary widgets. It returns a safe empty object
     * if the record is missing or invalid, preventing UI crashes.
     *
     * @param fy         The Fiscal Year.
     * @param periodName The Period name.
     * @return A {@link PeriodSubmissionData} object (never null).
     */
    @Override
    @Transactional(readOnly = true)
    public PeriodSubmissionData getDataByFyAndPeriod(Integer fy, String periodName) {
        Optional<Period> optionalPeriod = periodRepository.findByName(periodName);
        if (optionalPeriod.isEmpty()) return new PeriodSubmissionData(fy, periodName);

        Optional<Obligation> optionalObligation = obligationRepository.findByFiscalYearAndPeriod(fy, optionalPeriod.get());

        if (optionalObligation.isPresent()) {
            try {
                checkIfValid(optionalObligation.get());
                return new PeriodSubmissionData(optionalObligation.get());
            } catch (NotFoundRecordException e) {
                return new PeriodSubmissionData(fy, periodName); // Return 0s on error
            }
        }
        return new PeriodSubmissionData(fy, periodName);
    }

    /**
     * Retrieves PeriodSubmissionData for a past period calculated by offset.
     * <p>
     * Logic handles fiscal year rollovers (e.g., going back from Jan 2024 to Dec 2023).
     *
     * @param fy              The current Fiscal Year.
     * @param period          The current Period name.
     * @param periodsToGoBack The number of months/periods to subtract.
     * @return A {@link PeriodSubmissionData} object for the past target.
     */
    @Override
    @Transactional(readOnly = true)
    public PeriodSubmissionData getDataFromPastPeriod(Integer fy, String period, int periodsToGoBack) {
        if (periodsToGoBack <= 0) return getDataByFyAndPeriod(fy, period);

        Optional<Period> optionalCurrentPeriod = periodRepository.findByName(period);
        if (optionalCurrentPeriod.isEmpty()) return new PeriodSubmissionData(fy, period);

        int currentOrder = optionalCurrentPeriod.get().getOrder();
        int yearsToSubtract = periodsToGoBack / 12;
        int monthsToSubtract = periodsToGoBack % 12;
        int targetFiscalYear = fy - yearsToSubtract;
        int targetPeriodOrder = currentOrder - monthsToSubtract;

        if (targetPeriodOrder <= 0) {
            targetFiscalYear -= 1;
            targetPeriodOrder += 12;
        }

        Optional<Period> optionalTargetPeriod = periodRepository.findByOrder(targetPeriodOrder);
        if (optionalTargetPeriod.isEmpty()) {
            return new PeriodSubmissionData(targetFiscalYear, "PeriodOrder(" + targetPeriodOrder + ")");
        }

        return getDataByFyAndPeriod(targetFiscalYear, optionalTargetPeriod.get().getName());
    }

    /**
     * Retrieves and maps the active submission to a {@link SubmissionCustomDTO}.
     * This is the main entry point for the UI grid.
     */
    @Override
    @Transactional(readOnly = true)
    public List<SubmissionCustomDTO> getAllObligationsByFyAndPeriod(Integer fy, String period) throws NotFoundRecordException {
        Optional<Period> optionalPeriod = periodRepository.findByName(period);
        if (optionalPeriod.isEmpty()) return Collections.emptyList();

        Optional<Obligation> optionalObligation = obligationRepository.findByFiscalYearAndPeriod(fy, optionalPeriod.get());
        if (optionalObligation.isEmpty()) return Collections.emptyList();

        Obligation obligation = optionalObligation.get();
        List<Submission> submissions = submissionRepository.findByObligationId(obligation.getId());
        List<Submission> filteredSubmissions = submissions.stream()
                .filter(submission -> !submission.getCurrentSubmissionStatus().getName().equals(SubmissionStatusEnum.ERROR.getDbName()))
                .toList();

        List<SubmissionCustomDTO> customSubmissions = new ArrayList<>();
        for (Submission s : filteredSubmissions) {
            SubmissionCustomDTO dto = alternativeMapperFacade.map(s, SubmissionCustomDTO.class);
            dto.setValidations(getValidations(s));
            dto.setIngestionsCount(s.getIngestions().size());
            customSubmissions.add(dto);
        }

        return customSubmissions;
    }

    /**
     * Helper wrapper to return a List<Submission> instead of Optional.
     * Useful for batch processing or legacy compatibility.
     *
     * @param fy         The Fiscal Year.
     * @param periodName The Period Name.
     * @return List containing the single active submission, or empty list.
     * @throws NotFoundRecordException if validation fails.
     */
    @Override
    @Transactional(readOnly = true)
    public List<Submission> getSubmissionsForStats(Integer fy, String periodName) throws NotFoundRecordException {

        Optional<Obligation> obligationOpt = this.obligationRepository.findByFiscalYearAndPeriod_Name(fy, periodName);
        if (obligationOpt.isEmpty()) return Collections.emptyList();

        Obligation obligation = obligationOpt.get();

        List<Submission> submissions = this.submissionRepository.findByObligationId(obligation.getId());

        return submissions.stream()
                .filter(this::isActiveSubmission)
                .toList();
    }

    /**
     * Filters the submissions of an Obligation to find only the "Active" ones.
     * "Active" is defined as not Cancelled and not Rejected.
     *
     * @param obligation The obligation to filter.
     * @return List of active submissions.
     */
    @Override
    public List<Submission> getActiveSubmissions(Obligation obligation) {
        List<Submission> submissions = submissionRepository.findByObligationId(obligation.getId());

        return submissions.stream()
                .filter(this::isActiveSubmission)
                .toList();
    }

    private boolean isActiveSubmission(Submission submission) {
        // 1. Safety checks for nulls
        if (submission.getCurrentSubmissionStatus() == null ||
                submission.getCurrentSubmissionStatus().getOrder() == null) {
            return false;
        }

        Integer order = submission.getCurrentSubmissionStatus().getOrder();

        // 2. Use the Enum IDs for comparison
        // We check if the order is NOT Cancelled AND NOT Rejected
        return !order.equals(SubmissionStatusEnum.CANCELLED.getOrder()) &&
                !order.equals(SubmissionStatusEnum.REJECTED.getOrder());
    }

    /**
     * Aggregates validation counts (Errors and Warnings) for a submission.
     *
     * @param submission The submission to analyze.
     * @return List of ValidationDTO (one for errors, one for warnings).
     */
    private List<ValidationDTO> getValidations(Submission submission) {
        long errorCount = 0;
        long warningCount = 0;

        Ingestion ingestion = ingestionRepository.findFirstBySubmission_IdAndIngestionType_Name(
                submission.getId(), IngestionTypeEnum.SOGGETTI.getLabel()).orElse(null);

        if (ingestion == null) return Collections.emptyList();
        warningCount += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.WARNING.getLevel());
        errorCount += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.ERROR.getLevel());

        return Arrays.asList(
                new ValidationDTO(ERROR_TAG, errorCount),
                new ValidationDTO(WARNING_TAG, warningCount)
        );
    }

    @Override
    @Async
    public CompletableFuture<Void> ingestObligationFilesAsync() throws NotFoundRecordException, IOException {
        log.debug("Starting async ingestion of obligation files");
        try {
            // Verifica che S3Service sia disponibile nel thread asincrono
            if (s3Service == null) {
                log.error("S3Service is null in async thread");
                throw new IllegalStateException("S3Service is not available in async thread");
            }
            log.debug("S3Service is available in async thread, bucket: {}", s3Service.getBucketName());

            ingestObligationFilesForMerchants();
            log.debug("Async ingestion of obligation files completed successfully");
        } catch (Exception e) {
            log.error("Error during async ingestion of obligation files", e);
            throw e;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void ingestObligationFilesForMerchants() throws NotFoundRecordException {
        log.debug("Starting ingestion of obligation files");
//        List<String> fileNames = s3Service.fetchFileKeysFromBucket();
        List<String> fileNames = s3Service.fetchFileKeysFromBucketTest();
        log.info("Fetched {} file(s) from bucket: {}", fileNames.size(), fileNames);

        FileDescriptor fileProps = extractEotFileProps(fileNames);
        log.debug("Extracted file properties: year={}, month={}",
                fileProps.year(), fileProps.month());

        List<String> soggettiFiles = filterFiles(fileNames, "soggetti");
        List<String> rapportiFiles = filterFiles(fileNames, "rapporti");
        List<String> daticontabiliFiles = filterFiles(fileNames, "dati_contabili");
        List<String> collegamentiFiles = filterFiles(fileNames, "collegamenti");
        List<String> cambiondgFiles = filterFiles(fileNames, "cambio_ndg");
        log.debug("Soggetti files: {}, Rapporti files: {}, Dati Contabili files: {}, Collegamenti files: {}, Cambio NDG files: {}",
                soggettiFiles.size(), rapportiFiles.size(), daticontabiliFiles.size(), collegamentiFiles.size(), cambiondgFiles.size());

        Period period = periodRepository.findByName(fileProps.month()).orElse(null);
        Obligation obligation = findOrCreateObligation(fileProps.year(), period);
        log.debug("Using obligation: id={}, fiscalYear={}, period={}",
                obligation.getId(), obligation.getFiscalYear(),
                period != null ? period.getName() : "null");

        boolean hasActiveSubmission = hasActiveSubmission(obligation);
        log.info("hasActiveSubmission check result: {} for obligation id={}, fiscalYear={}, period={}",
                hasActiveSubmission, obligation.getId(), obligation.getFiscalYear(),
                period != null ? period.getName() : "null");

        if (hasActiveSubmission) {
            log.warn("Found active submission for obligation id={}, fiscalYear={}, period={}. " +
                            "Cannot start new ingestion. Please cancel/close existing submissions first.",
                    obligation.getId(), obligation.getFiscalYear(),
                    period != null ? period.getName() : "null");
        }

        Submission submission = this.submissionService.createSubmissionByObligation(obligation);
        log.info("Created submission: id={} with status: {}",
                submission.getId(),
                submission.getCurrentSubmissionStatus() != null ? submission.getCurrentSubmissionStatus().getName() : "null");

        if (hasActiveSubmission) {
            log.error("Aborting ingestion - active submission exists for obligation id={}. Marking new submission as ERROR.",
                    obligation.getId());
            submissionService.markAsError(submission);
            return;
        }

        log.info("No active submissions found. Proceeding with ingestion for submission id={}", submission.getId());

        // Update status to DATA_VALIDATION when starting data processing
        try {
            submissionService.updateStatusInternal(submission, 2); // DATA_VALIDATION
            log.info("Updated submission {} status to DATA_VALIDATION - starting data processing", submission.getId());
        } catch (NotFoundRecordException e) {
            log.error("Failed to update submission status to DATA_VALIDATION", e);
            submissionService.markAsError(submission);
            return;
        }

        AtomicReference<Ingestion> ingestionRef = new AtomicReference<>();

        try {
            if (useStagingIngestion) {
                // =====================================================
                // HIGH-PERFORMANCE STAGING APPROACH (recommended for large files)
                // Uses bulk load + set-based operations for 10-20x speedup
                // =====================================================
                log.info("Using STAGING-BASED ingestion (high-performance ETL approach)");
                processFilesWithStagingApproachForMerchants(collegamentiFiles, soggettiFiles, rapportiFiles,
                        daticontabiliFiles, cambiondgFiles, submission, obligation, ingestionRef);
            } else {
                // =====================================================
                // LEGACY ROW-BY-ROW APPROACH (kept for compatibility)
                // =====================================================
                log.info("Using LEGACY row-by-row ingestion approach");
                processFilesWithLegacyApproach(collegamentiFiles, soggettiFiles, rapportiFiles,
                        daticontabiliFiles, cambiondgFiles, submission, obligation, ingestionRef);
            }
//            s3Service.moveFileFromInputToInputLoaded(fileProps.eotFileName());

            // All files processed successfully - update status to VALIDATION_COMPLETED
            try {
                submissionService.updateStatusInternal(submission, 3); // VALIDATION_COMPLETED
                log.info("Updated submission {} status to VALIDATION_COMPLETED - all data processed successfully", submission.getId());
            } catch (NotFoundRecordException e) {
                log.error("Failed to update submission status to VALIDATION_COMPLETED", e);
                // Don't mark as error, just log - the data is already processed
            }
        } catch (Exception exception) {
            log.error("An error occured during ingestion: {}", exception.getMessage(), exception);
            cleanUpFailedSubmission(submission, ingestionRef.get(), exception);
        }
    }

    private boolean hasActiveSubmission(Obligation obligation) {
        List<Submission> submissions = submissionRepository.findByObligationId(obligation.getId());
        log.debug("Found {} total submission(s) for obligation id={}", submissions.size(), obligation.getId());

        List<Submission> activeSubmissions = submissions.stream().filter(s -> {
            String statusName = s.getCurrentSubmissionStatus().getName();
            boolean isActive = !statusName.equals(SubmissionStatusEnum.CANCELLED.getDbName())
                    && !statusName.equals(SubmissionStatusEnum.ERROR.getDbName())
                    && !statusName.equals(SubmissionStatusEnum.REJECTED.getDbName());
            if (isActive) {
                log.debug("Active submission found: id={}, status={}", s.getId(), statusName);
            }
            return isActive;
        }).toList();

        log.info("hasActiveSubmission for obligation id={}: {} active submission(s) found",
                obligation.getId(), activeSubmissions.size());
        return !activeSubmissions.isEmpty();
    }

    private String getEotFileName(List<String> files) throws NotFoundRecordException {
        log.debug("Searching for EOT file in {} file(s)", files.size());
        for (String file : files) {
            if (file != null) {
                String name = file.toLowerCase();
                log.debug("Checking file: {}", file);
                if (name.endsWith(".eot")) {
                    log.debug("Found EOT file: {}", file);
                    return file;
                }
            }
        }
        log.error("EOT file not found in the provided files list");
        throw new NotFoundRecordException(".eot file is not found");
    }

    private List<String> filterFiles(List<String> files, String substring) {
        log.debug("Filtering .txt files containing {} in {} file(s)", substring, files.size());
        List<String> filteredFiles = files.stream()
                .filter(Objects::nonNull)
                .filter(file -> file.toLowerCase().endsWith(".txt"))
                .filter(file -> file.toLowerCase().contains(substring))
                .toList();
        log.debug("Found {} {} file(s)", filteredFiles.size(), substring);
        return filteredFiles;
    }

    private List<RemoteFile> fetchFilesFromBucket() throws IOException, NotFoundRecordException {
        // uncomment for local testing
//        List<RemoteFile> files = new ArrayList<>();
//        List<String> resourceNames = new ArrayList<>(
//                List.of("TRXPOSADE_32875_TRANSATOPOS_202510_20251127162800.txt",
//                        "TRXPOSADE_32875_TRANSATOPOS_202510_20251127162800.eot",
//                        "TRXPOSADE_32875_ANAGRAFEPOS_202510_20251127162800.txt"
//                ));
//
//        log.debug("Fetching files from bucket");
//        for (String resourceName : resourceNames) {
//            Resource resource = new ClassPathResource(resourceName);
//            if (!resource.exists()) {
//                log.error("Resource '{}' not found in classpath resources", resourceName);
//                throw new NotFoundRecordException(resourceName + " not found in classpath resources");
//            }
//            log.debug("Successfully loaded resource: {}", resourceName);
//            files.add(new RemoteFile(resourceName, resource.getInputStream()));
//        }
//        log.debug("Created {} RemoteFile(s) from resource", files.size());
//        return files;


        // NUOVO CODICE: Lettura file da S3
        log.debug("Fetching files from S3 bucket, input folder: {}", inputFolder);
        List<RemoteFile> files = new ArrayList<>();

        // Lista tutti gli oggetti nella cartella input-folder
        List<S3Object> s3Objects = s3Service.listObjects(inputFolder);

        if (s3Objects.isEmpty()) {
            log.warn("No files found in S3 input folder: {}", inputFolder);
            throw new NotFoundRecordException("No files found in S3 input folder: " + inputFolder);
        }

        log.debug("Found {} object(s) in S3 input folder", s3Objects.size());

        // Per ogni oggetto S3, scarica il file e crea un RemoteFile
        for (S3Object s3Object : s3Objects) {
            String key = s3Object.key();
            // Estrae solo il nome del file dalla chiave (rimuove il path della cartella)
            String fileName = key.substring(key.lastIndexOf('/') + 1);

            log.debug("Downloading file from S3: {}", key);
            InputStream inputStream = s3Service.downloadFileAsStream(key);
            files.add(new RemoteFile(fileName, inputStream));
            log.debug("Successfully created RemoteFile for: {}", fileName);
        }

        log.debug("Created {} RemoteFile(s) from S3", files.size());
        return files;
    }

    // Retry configuration for lock timeout
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 100; // Start with 100ms

    public void saveProcessedBatches(ProcessedRecordBatch batch, Ingestion ingestion, Submission submission) {
        log.info("Starting to save processed batch in main thread for ingestionId: {}",
                ingestion.getId());

        if (!batch.soggettiList().isEmpty()) {
            executeWithRetry(() -> {
                soggettiRepository.bulkInsert(batch.soggettiList(), submission);
            }, "soggetti");
        }
        
        if (!batch.rapportiList().isEmpty()) {
            executeWithRetry(() -> {
                rapportiRepository.bulkInsert(batch.rapportiList(), submission);
            }, "rapporti");
            
            // Update ADE_RAPPORTO_IDENTIFIER for newly inserted rapporti
            executeWithRetry(() -> {
                int updated = rapportiRepository.bulkUpdateAdeRapportoIdentifier(submission.getId());
                log.info("Updated {} ADE_RAPPORTO_IDENTIFIER values for rapporti", updated);
            }, "rapporti identifier update");
        }
        
        if (!batch.errorRecords().isEmpty()) {
            executeWithRetry(() -> {
                errorRecordRepository.bulkInsertRecordsWithCauses(batch.errorRecords(), ingestion.getId());
                log.info("Saved {} error records", batch.errorRecords().size());
            }, "error records");
        }
    }

    /**
     * Execute database operation with retry logic for lock timeout errors.
     * Uses exponential backoff: 100ms, 200ms, 400ms
     */
    private void executeWithRetry(Runnable operation, String operationName) {
        int attempt = 0;
        long delay = INITIAL_RETRY_DELAY_MS;

        while (attempt < MAX_RETRIES) {
            try {
                operation.run();
                return; // Success, exit retry loop
            } catch (Exception e) {
                attempt++;

                // Check if it's a lock timeout error
                boolean isLockTimeout = e.getMessage() != null &&
                        (e.getMessage().contains("Lock wait timeout") ||
                                e.getMessage().contains("try restarting transaction"));

                if (isLockTimeout && attempt < MAX_RETRIES) {
                    log.warn("Lock timeout on {} (attempt {}/{}), retrying after {}ms",
                            operationName, attempt, MAX_RETRIES, delay);
                    try {
                        Thread.sleep(delay);
                        delay *= 2; // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                } else {
                    // Not a lock timeout or max retries reached, rethrow
                    log.error("Failed to save {} after {} attempts", operationName, attempt, e);
                    throw new RuntimeException("Failed to save " + operationName + " after " + attempt + " attempts", e);
                }
            }
        }
    }

    private FileDescriptor extractEotFileProps(List<String> files) throws NotFoundRecordException {
        String eotFileName = getEotFileName(files);
        log.debug("Found EOT file: {}", eotFileName);

        log.debug("Extracting file properties from filename: {}", eotFileName);
        String[] parts = eotFileName.split("_");
        if (parts.length < 5) {
            throw new IllegalArgumentException("Invalid filename structure: " + eotFileName);
        }

        String typeName = parts[2].toLowerCase();
        log.debug("Extracted type name: {}", typeName);

        String datePart = parts[3];
        log.debug("Extracted date part: {}", datePart);

        if (!datePart.matches("^[0-9]{6}$")) {
            throw new IllegalArgumentException("Invalid date segment (expected CCYYMM): " + datePart);
        }

        int year = Integer.parseInt(datePart.substring(0, 4));
        int monthNum = Integer.parseInt(datePart.substring(4, 6));
        log.debug("Parsed year: {}, month number: {}", year, monthNum);

        String month = Month.of(monthNum).getDisplayName(TextStyle.FULL, Locale.ENGLISH);
        log.debug("Converted month number to name: {}", month);

        FileDescriptor descriptor = new FileDescriptor(year, month, eotFileName);
        log.debug(
                "Successfully extracted file properties: year={}, month={}",
                year, month
        );

        return descriptor;
    }

    private Obligation findOrCreateObligation(int year, Period period) {
        log.debug("Finding or creating obligation for year={}, period={}",
                year, period != null ? period.getName() : "null");
        Obligation oldObligation = obligationRepository.findByFiscalYearAndPeriod(year, period)
                .orElseGet(() -> {
                    log.debug("Creating new obligation for year={}, period={}",
                            year, period != null ? period.getName() : "null");
                    Obligation newObligation = new Obligation();
                    newObligation.setFiscalYear(year);
                    newObligation.setPeriod(period);

                    Obligation savedObligation = obligationRepository.save(newObligation);
                    savedObligation.setPeriod(period);
                    return savedObligation;
                });
        oldObligation.setPeriod(period);
        return oldObligation;
    }

//    /**
//     * HIGH-PERFORMANCE STAGING APPROACH
//     * Uses bulk load into staging tables + set-based operations for 10-20x speedup.
//     * Ideal for large files (250k+ merchants, 800k+ transactions).
//     */
//    private void processFilesWithStagingApproach(
//            List<String> anagrafeFiles,
//            List<String> transatoFiles,
//            Submission submission,
//            Obligation obligation) throws NotFoundRecordException, IOException {
//
//        long totalStartTime = System.currentTimeMillis();
//
//        // Phase 1: Process merchant files (anagrafe)
//        log.info("=== STAGING Phase 1: Processing {} merchant file(s) ===", anagrafeFiles.size());
//        IngestionType anagrafeType = ingestionTypeRepository.findByNameIgnoreCase("anagrafe")
//                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'anagrafe' is not found"));
//
//        int totalMerchantsInserted = 0;
//        int totalMerchantsDuplicate = 0;
//
//        for (String keyName : anagrafeFiles) {
//            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
//            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
//                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);
//
//                log.info("Processing merchant file: {}", fileName);
//                Ingestion ingestion = this.ingestionService.createIngestionBySubmission(submission, anagrafeType);
//
//                long fileStartTime = System.currentTimeMillis();
//                StagingResult result = stagingIngestionService.processSoggettiFile(remoteFile, ingestion, submission);
//                long fileElapsed = System.currentTimeMillis() - fileStartTime;
//
//                totalMerchantsInserted += result.insertedCount();
//                totalMerchantsDuplicate += result.duplicateCount();
//
//                log.info("Completed merchant file {} in {}ms: inserted={}, duplicates={}",
//                        fileName, fileElapsed, result.insertedCount(), result.duplicateCount());
//
//                ingestionService.markAsSuccess(ingestion);
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());
//            }
//        }
//
//        log.info("=== STAGING Phase 1 Complete: {} merchants inserted, {} duplicates ===",
//                totalMerchantsInserted, totalMerchantsDuplicate);
//
//        // Phase 2: Process transaction files (transato)
//        log.info("=== STAGING Phase 2: Processing {} transaction file(s) ===", transatoFiles.size());
//        IngestionType transatoType = ingestionTypeRepository.findByNameIgnoreCase("transato")
//                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'transato' is not found"));
//
//        int totalTransactionsInserted = 0;
//        int totalTransactionsDuplicate = 0;
//        int totalMissingMerchants = 0;
//
//        for (String keyName : transatoFiles) {
//            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
//            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
//                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);
//
//                log.info("Processing transaction file: {}", fileName);
//                Ingestion ingestion = this.ingestionService.createIngestionBySubmission(submission, transatoType);
//
//                long fileStartTime = System.currentTimeMillis();
//                StagingResult result = stagingIngestionService.processTransactionFile(remoteFile, ingestion, submission, obligation);
//                long fileElapsed = System.currentTimeMillis() - fileStartTime;
//
//                totalTransactionsInserted += result.insertedCount();
//                totalTransactionsDuplicate += result.duplicateCount();
//                totalMissingMerchants += result.missingMerchantCount();
//
//                log.info("Completed transaction file {} in {}ms: inserted={}, duplicates={}, missingMerchants={}",
//                        fileName, fileElapsed, result.insertedCount(), result.duplicateCount(), result.missingMerchantCount());
//
//                ingestionService.markAsSuccess(ingestion);
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());
//
//                // Incremental cleanup: clear transaction staging after each file to free memory
//                stagingIngestionService.cleanupTransactionStaging(submission.getId());
//            }
//        }
//
//        log.info("=== STAGING Phase 2 Complete: {} transactions inserted, {} duplicates, {} missing merchants ===",
//                totalTransactionsInserted, totalTransactionsDuplicate, totalMissingMerchants);
//
//        // Final cleanup: clear merchant staging (transactions already cleared incrementally)
//        stagingIngestionService.cleanupStaging(submission.getId());
//
//        long totalElapsed = System.currentTimeMillis() - totalStartTime;
//        log.info("=== STAGING INGESTION COMPLETE in {}ms ({} seconds) ===", totalElapsed, totalElapsed / 1000);
//        log.info("Summary: merchants={}/{} inserted/dup, transactions={}/{}/{} inserted/dup/missing",
//                totalMerchantsInserted, totalMerchantsDuplicate,
//                totalTransactionsInserted, totalTransactionsDuplicate, totalMissingMerchants);
//    }

    private void processFilesWithStagingApproachForMerchants(
            List<String> collegamentiFiles,
            List<String> soggettiFiles,
            List<String> rapportiFiles,
            List<String> daticontabiliFiles,
            List<String> cambiondgFiles,
            Submission submission,
            Obligation obligation,
            AtomicReference<Ingestion> ingestionRef) throws NotFoundRecordException, IOException {

        long totalStartTime = System.currentTimeMillis();


        // Phase 1: Process Collegamenti files (PARENT ENTITY - MUST BE FIRST!)
        log.info("=== STAGING Phase 1: Processing {} collegamenti file(s) (PARENT) ===", collegamentiFiles.size());
        IngestionType collegamentiType = ingestionTypeRepository.findByNameIgnoreCase("Collegamenti")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'collegamenti' is not found"));

        int totalCollegamentiInserted = 0;
        int totalCollegamentiDuplicate = 0;

        for (String keyName : collegamentiFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStreamTest("COLLEGAMENTI.txt")) {
//            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.info("Processing collegamenti file: {}", fileName);
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, collegamentiType));

                long fileStartTime = System.currentTimeMillis();
                StagingResult result = stagingIngestionService.processCollegamentiFile(remoteFile, ingestionRef.get(), submission);
                long fileElapsed = System.currentTimeMillis() - fileStartTime;

                totalCollegamentiInserted += result.insertedCount();
                totalCollegamentiDuplicate += result.duplicateCount();

                log.info("Completed collegamenti file {} in {}ms: inserted={}, duplicates={}",
                        fileName, fileElapsed, result.insertedCount(), result.duplicateCount());

                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());

                stagingIngestionService.cleanupStaging(submission.getId());
            }
        }

        log.info("=== STAGING Phase 1 Complete: {} collegamenti inserted, {} duplicates ===",
                totalCollegamentiInserted, totalCollegamentiDuplicate);


        // Phase 2: Process Soggetti files (CHILD - requires Collegamenti.ndg)
        log.info("=== STAGING Phase 2: Processing {} soggetti file(s) (CHILD) ===", soggettiFiles.size());
        IngestionType soggettiType = ingestionTypeRepository.findByNameIgnoreCase("soggetti")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'soggetti' is not found"));

        int totalSoggettiInserted = 0;
        int totalSoggettiDuplicate = 0;

        for (String keyName : soggettiFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStreamTest("SOGGETTI.txt")) {
//            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.info("Processing soggetti file: {}", fileName);
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, soggettiType));

                long fileStartTime = System.currentTimeMillis();
                StagingResult result = stagingIngestionService.processSoggettiFile(remoteFile, ingestionRef.get(), submission);
                long fileElapsed = System.currentTimeMillis() - fileStartTime;

                totalSoggettiInserted += result.insertedCount();
                totalSoggettiDuplicate += result.duplicateCount();

                log.info("Completed soggetti file {} in {}ms: inserted={}, duplicates={}",
                        fileName, fileElapsed, result.insertedCount(), result.duplicateCount());

                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());

                stagingIngestionService.cleanupStaging(submission.getId());
            }
        }

        log.info("=== STAGING Phase 2 Complete: {} soggetti inserted, {} duplicates ===",
                totalSoggettiInserted, totalSoggettiDuplicate);

        // Phase 3: Process Rapporti files (CHILD - requires Collegamenti.chiave_rapporto)
        log.info("=== STAGING Phase 3: Processing {} rapporti file(s) (CHILD) ===", rapportiFiles.size());
        IngestionType rapportiType = ingestionTypeRepository.findByNameIgnoreCase("rapporti")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'rapporti' is not found"));

        int totalRapportiInserted = 0;
        int totalRapportiDuplicate = 0;

        for (String keyName : rapportiFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStreamTest("RAPPORTI.txt")) {
//            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.info("Processing rapporti file: {}", fileName);
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, rapportiType));

                long fileStartTime = System.currentTimeMillis();
                StagingResult result = stagingIngestionService.processRapportiFile(remoteFile, ingestionRef.get(), submission);
                long fileElapsed = System.currentTimeMillis() - fileStartTime;

                totalRapportiInserted += result.insertedCount();
                totalRapportiDuplicate += result.duplicateCount();

                log.info("Completed rapporti file {} in {}ms: inserted={}, duplicates={}",
                        fileName, fileElapsed, result.insertedCount(), result.duplicateCount());

                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());

                stagingIngestionService.cleanupStaging(submission.getId());
            }
        }

        log.info("=== STAGING Phase 3 Complete: {} rapporti inserted, {} duplicates ===",
                totalRapportiInserted, totalRapportiDuplicate);

        // Phase 4: Process Dati Contabili files (CHILD - requires Collegamenti.chiave_rapporto)
        log.info("=== STAGING Phase 4: Processing {} daticontabili file(s) (CHILD) ===", daticontabiliFiles.size());
        IngestionType daticontabiliType = ingestionTypeRepository.findByNameIgnoreCase("datiContabili")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'daticontabili' is not found"));

        int totalDatiContabiliInserted = 0;
        int totalDatiContabiliDuplicate = 0;

        for (String keyName : daticontabiliFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStreamTest(keyName)) {
//            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.info("Processing daticontabili file: {}", fileName);
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, daticontabiliType));

                long fileStartTime = System.currentTimeMillis();
                StagingResult result = stagingIngestionService.processDaticontabiliFile(remoteFile, ingestionRef.get(), submission);
                long fileElapsed = System.currentTimeMillis() - fileStartTime;

                totalDatiContabiliInserted += result.insertedCount();
                totalDatiContabiliDuplicate += result.duplicateCount();

                log.info("Completed daticontabili file {} in {}ms: inserted={}, duplicates={}",
                        fileName, fileElapsed, result.insertedCount(), result.duplicateCount());

                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());

                stagingIngestionService.cleanupStaging(submission.getId());
            }
        }

        log.info("=== STAGING Phase 4 Complete: {} daticontabili inserted, {} duplicates ===",
                totalDatiContabiliInserted, totalDatiContabiliDuplicate);


        // Phase 5: Process Cambio NDG files (INDEPENDENT - no FK constraints)
        log.info("=== STAGING Phase 5: Processing {} cambiondg file(s) (INDEPENDENT) ===", cambiondgFiles.size());
        IngestionType cambiondgType = ingestionTypeRepository.findByNameIgnoreCase("cambioNdg")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'cambiondg' is not found"));

        int totalCambioNdgInserted = 0;
        int totalCambioNdgDuplicate = 0;

        for (String keyName : cambiondgFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStreamTest("CAMBIO_NDG.txt")) {
//            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.info("Processing cambiondg file: {}", fileName);
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, cambiondgType));

                long fileStartTime = System.currentTimeMillis();
                StagingResult result = stagingIngestionService.processCambioNdgFile(remoteFile, ingestionRef.get(), submission);
                long fileElapsed = System.currentTimeMillis() - fileStartTime;

                totalCambioNdgInserted += result.insertedCount();
                totalCambioNdgDuplicate += result.duplicateCount();

                log.info("Completed cambiondg file {} in {}ms: inserted={}, duplicates={}",
                        fileName, fileElapsed, result.insertedCount(), result.duplicateCount());

                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());

                stagingIngestionService.cleanupStaging(submission.getId());
            }
        }

        log.info("=== STAGING Phase 5 Complete: {} cambiondg inserted, {} duplicates ===",
                totalCambioNdgInserted, totalCambioNdgDuplicate);

        stagingIngestionService.cleanupStaging(submission.getId());

        // =====================================================
        // ORPHAN VALIDATION PHASE
        // Validate Collegamenti have corresponding Soggetti and Rapporti children
        // Delete orphans and cascade delete to orphaned children
        // =====================================================
        log.info("=== STAGING Orphan Validation: Checking Collegamenti integrity ===");
        OrphanValidationResult orphanResult = validateAndCleanOrphanCollegamenti(submission, ingestionRef.get());
        log.info("=== STAGING Orphan Validation Complete: {} total records deleted (Collegamenti: {}, Soggetti: {}, Rapporti: {}) ===",
                orphanResult.totalDeleted(), orphanResult.collegamentiDeleted(), orphanResult.soggettiDeleted(), orphanResult.rapportiDeleted());

        long totalElapsed = System.currentTimeMillis() - totalStartTime;
        log.info("=== STAGING INGESTION COMPLETE in {}ms ({} seconds) ===", totalElapsed, totalElapsed / 1000);
        log.info("Summary: collegamenti={}/{} inserted/dup, soggetti={}/{} inserted/dup, rapporti={}/{} inserted/dup, daticontabili={}/{} inserted/dup, cambiondg={}/{} inserted/dup",
                totalCollegamentiInserted, totalCollegamentiDuplicate,
                totalSoggettiInserted, totalSoggettiDuplicate,
                totalRapportiInserted, totalRapportiDuplicate,
                totalDatiContabiliInserted, totalDatiContabiliDuplicate,
                totalCambioNdgInserted, totalCambioNdgDuplicate);
    }

    /**
     * LEGACY ROW-BY-ROW APPROACH
     * Original implementation with per-batch duplicate checks.
     * Kept for compatibility and rollback capability.
     */
//    private void processFilesWithLegacyApproach(
//            List<String> anagrafeFiles,
//            List<String> transatoFiles,
//            Submission submission,
//            Obligation obligation) throws NotFoundRecordException, IOException {
//
//        Ingestion currentIngestion = null;
//
//        log.info("Phase 1: Processing {} anagrafe file(s) (merchants)", anagrafeFiles.size());
//        IngestionType anagrafeType = ingestionTypeRepository.findByNameIgnoreCase("anagrafe")
//                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'anagrafe' is not found"));
//
//        for (String keyName : anagrafeFiles) {
//            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
//            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
//                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);
//
//                log.debug("Processing anagrafe file: {}", fileName);
//                currentIngestion = this.ingestionService.createIngestionBySubmission(submission, anagrafeType);
//                log.debug("Created ingestion: id={} for file: {}", currentIngestion.getId(), fileName);
//
//                long anagrafeProcessStart = System.nanoTime();
//
//                transactionFileProcessingService.processWithImmediateSave(
//                        remoteFile, currentIngestion, submission, obligation, this);
//
//                long anagrafeProcessFinish = System.nanoTime();
//
//                long processingTime = Duration.ofNanos(anagrafeProcessFinish - anagrafeProcessStart).toSeconds();
//                log.debug("Completed processing anagrafe file: {}, processing time: {} seconds",
//                        remoteFile.name(), processingTime);
//                ingestionService.markAsSuccess(currentIngestion);
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());
//            }
//        }
//
//        log.info("Phase 2: Processing {} transato file(s) (transactions)", transatoFiles.size());
//
//        IngestionType transatoType = ingestionTypeRepository.findByNameIgnoreCase("transato")
//                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'transato' is not found"));
//
//        for (String keyName : transatoFiles) {
//            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
//            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
//                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);
//
//                log.debug("Processing transato file: {}", remoteFile.name());
//                currentIngestion = this.ingestionService.createIngestionBySubmission(submission, transatoType);
//                log.debug("Created ingestion: id={} for file: {}", currentIngestion.getId(), remoteFile.name());
//
//                long transatoProcessStart = System.nanoTime();
//
//                // Process file with immediate save to avoid OutOfMemory
//                transactionFileProcessingService.processWithImmediateSave(
//                        remoteFile, currentIngestion, submission, obligation, this);
//
//                long transatoProcessFinish = System.nanoTime();
//
//                long transatoProcessingTime = Duration.ofNanos(transatoProcessFinish - transatoProcessStart).toSeconds();
//                log.debug("Completed processing transato file: {}, processing time: {} seconds",
//                        remoteFile.name(), transatoProcessingTime);
//                ingestionService.markAsSuccess(currentIngestion);
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());
//            }
//        }
//    }
    private void processFilesWithLegacyApproach(
            List<String> collegamentiFiles,
            List<String> soggettiFiles,
            List<String> rapportiFiles,
            List<String> daticontabiliFiles,
            List<String> cambiondgFiles,
            Submission submission,
            Obligation obligation,
            AtomicReference<Ingestion> ingestionRef) throws NotFoundRecordException, IOException {

//        Ingestion currentIngestion = null;

        // Phase 1: Process Collegamenti files (PARENT - MUST BE FIRST!)
        log.info("Phase 1: Processing {} collegamenti file(s) (PARENT)", collegamentiFiles.size());
        IngestionType collegamentiType = ingestionTypeRepository.findByNameIgnoreCase("collegamenti")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'collegamenti' is not found"));

        for (String keyName : collegamentiFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.debug("Processing collegamenti file: {}", remoteFile.name());
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, collegamentiType));
                log.debug("Created ingestion: id={} for file: {}", ingestionRef.get().getId(), remoteFile.name());

                long collegamentiProcessStart = System.nanoTime();

                merchantFileProcessingService.processWithImmediateSave(
                        remoteFile, ingestionRef.get(), submission, obligation, this);

                long collegamentiProcessFinish = System.nanoTime();

                long collegamentiProcessingTime = Duration.ofNanos(collegamentiProcessFinish - collegamentiProcessStart).toSeconds();
                log.debug("Completed processing collegamenti file: {}, processing time: {} seconds",
                        remoteFile.name(), collegamentiProcessingTime);
                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());
            }
        }

        // Phase 2: Process Soggetti files (CHILD - requires Collegamenti.ndg)
        log.info("Phase 2: Processing {} soggetti file(s) (CHILD)", soggettiFiles.size());
        IngestionType soggettiType = ingestionTypeRepository.findByNameIgnoreCase("soggetti")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'soggetti' is not found"));

        for (String keyName : soggettiFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.debug("Processing soggetti file: {}", remoteFile.name());
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, soggettiType));
                log.debug("Created ingestion: id={} for file: {}", ingestionRef.get().getId(), remoteFile.name());

                long soggettiProcessStart = System.nanoTime();

                merchantFileProcessingService.processWithImmediateSave(
                        remoteFile, ingestionRef.get(), submission, obligation, this);

                long soggettiProcessFinish = System.nanoTime();

                long soggettiProcessingTime = Duration.ofNanos(soggettiProcessFinish - soggettiProcessStart).toSeconds();
                log.debug("Completed processing soggetti file: {}, processing time: {} seconds",
                        remoteFile.name(), soggettiProcessingTime);
                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());
            }
        }

        // Phase 3: Process Rapporti files (CHILD - requires Collegamenti.chiave_rapporto)
        log.info("Phase 3: Processing {} rapporti file(s) (CHILD)", rapportiFiles.size());
        IngestionType rapportiType = ingestionTypeRepository.findByNameIgnoreCase("rapporti")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'rapporti' is not found"));

        for (String keyName : rapportiFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.debug("Processing rapporti file: {}", remoteFile.name());
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, rapportiType));
                log.debug("Created ingestion: id={} for file: {}", ingestionRef.get().getId(), remoteFile.name());

                long rapportiProcessStart = System.nanoTime();

                merchantFileProcessingService.processWithImmediateSave(
                        remoteFile, ingestionRef.get(), submission, obligation, this);

                long rapportiProcessFinish = System.nanoTime();

                long rapportiProcessingTime = Duration.ofNanos(rapportiProcessFinish - rapportiProcessStart).toSeconds();
                log.debug("Completed processing rapporti file: {}, processing time: {} seconds",
                        remoteFile.name(), rapportiProcessingTime);
                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());
            }
        }

        // Phase 4: Process Dati Contabili files (CHILD - requires Collegamenti.chiave_rapporto)
        log.info("Phase 4: Processing {} daticontabili file(s) (CHILD)", daticontabiliFiles.size());
        IngestionType daticontabiliType = ingestionTypeRepository.findByNameIgnoreCase("daticontabili")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'daticontabili' is not found"));

        for (String keyName : daticontabiliFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.debug("Processing daticontabili file: {}", remoteFile.name());
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, daticontabiliType));
                log.debug("Created ingestion: id={} for file: {}", ingestionRef.get().getId(), remoteFile.name());

                long daticontabiliProcessStart = System.nanoTime();

                merchantFileProcessingService.processWithImmediateSave(
                        remoteFile, ingestionRef.get(), submission, obligation, this);

                long daticontabiliProcessFinish = System.nanoTime();

                long daticontabiliProcessingTime = Duration.ofNanos(daticontabiliProcessFinish - daticontabiliProcessStart).toSeconds();
                log.debug("Completed processing daticontabili file: {}, processing time: {} seconds",
                        remoteFile.name(), daticontabiliProcessingTime);
                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());
            }
        }

        // Phase 5: Process Cambio NDG files (INDEPENDENT - no FK constraints)
        log.info("Phase 5: Processing {} cambiondg file(s) (INDEPENDENT)", cambiondgFiles.size());
        IngestionType cambiondgType = ingestionTypeRepository.findByNameIgnoreCase("cambiondg")
                .orElseThrow(() -> new NotFoundRecordException("Ingestion type 'cambiondg' is not found"));

        for (String keyName : cambiondgFiles) {
            String fileName = keyName.substring(keyName.lastIndexOf('/') + 1);
            try (InputStream inputStream = s3Service.downloadFileAsStream(keyName)) {
                RemoteFile remoteFile = new RemoteFile(fileName, inputStream);

                log.debug("Processing cambiondg file: {}", remoteFile.name());
                ingestionRef.set(this.ingestionService.createIngestionBySubmission(submission, cambiondgType));
                log.debug("Created ingestion: id={} for file: {}", ingestionRef.get().getId(), remoteFile.name());

                long cambiondgProcessStart = System.nanoTime();

                merchantFileProcessingService.processWithImmediateSave(
                        remoteFile, ingestionRef.get(), submission, obligation, this);

                long cambiondgProcessFinish = System.nanoTime();

                long cambiondgProcessingTime = Duration.ofNanos(cambiondgProcessFinish - cambiondgProcessStart).toSeconds();
                log.debug("Completed processing cambiondg file: {}, processing time: {} seconds",
                        remoteFile.name(), cambiondgProcessingTime);
                ingestionService.markAsSuccess(ingestionRef.get());
//                s3Service.moveFileFromInputToInputLoaded(remoteFile.name());
            }
        }

        // =====================================================
        // ORPHAN VALIDATION PHASE
        // Validate Collegamenti have corresponding Soggetti and Rapporti children
        // Delete orphans and cascade delete to orphaned children
        // =====================================================
        log.info("=== LEGACY Orphan Validation: Checking Collegamenti integrity ===");
        OrphanValidationResult orphanResult = validateAndCleanOrphanCollegamenti(submission, ingestionRef.get());
        log.info("=== LEGACY Orphan Validation Complete: {} total records deleted (Collegamenti: {}, Soggetti: {}, Rapporti: {}) ===",
                orphanResult.totalDeleted(), orphanResult.collegamentiDeleted(), orphanResult.soggettiDeleted(), orphanResult.rapportiDeleted());
    }

    /**
     * Validates and cleans orphan Collegamenti records and their children to ensure equal counts.
     * 
     * REQUIREMENT: count(Collegamenti) = count(Soggetti) = count(Rapporti)
     * 
     * Process:
     * 1. Find Collegamenti missing Soggetti OR Rapporti (or both) → Mark for deletion
     * 2. Find Soggetti whose Collegamenti parent will be deleted → Mark for deletion
     * 3. Find Rapporti whose Collegamenti parent will be deleted → Mark for deletion
     * 4. Create error records for all marked records
     * 5. Delete in correct order: Soggetti → Rapporti → Collegamenti
     * 
     * This ensures that every Collegamenti has exactly ONE Soggetti AND ONE Rapporti,
     * resulting in equal counts across all three tables.
     * 
     * @param submission The submission to validate
     * @param ingestion The ingestion to associate error records with
     * @return OrphanValidationResult containing counts of deleted records
     */
    @Transactional
    OrphanValidationResult validateAndCleanOrphanCollegamenti(Submission submission, Ingestion ingestion) {
        log.info("Starting orphan Collegamenti validation for submission: {} (ensuring equal counts)", submission.getId());
        
        long startTime = System.currentTimeMillis();
        
        // Step 1: Find Collegamenti missing Soggetti OR Rapporti (or both)
        List<Object[]> orphanCollegamenti = collegamentiRepository.findOrphanCollegamenti(submission.getId());
        
        if (orphanCollegamenti.isEmpty()) {
            log.info("No orphan Collegamenti found - all Collegamenti have both Soggetti and Rapporti children");
            return new OrphanValidationResult(0, 0, 0);
        }
        
        log.warn("Found {} orphan Collegamenti records (missing Soggetti OR Rapporti or both) for submission: {}", 
                orphanCollegamenti.size(), submission.getId());
        
        // Step 2: Find Soggetti that will become orphaned when Collegamenti is deleted
        List<Object[]> soggettiToOrphan = collegamentiRepository.findSoggettiToOrphan(submission.getId());
        log.info("Found {} Soggetti records that will be orphaned when Collegamenti is deleted", soggettiToOrphan.size());
        
        // Step 3: Find Rapporti that will become orphaned when Collegamenti is deleted
        List<Object[]> rapportiToOrphan = collegamentiRepository.findRapportiToOrphan(submission.getId());
        log.info("Found {} Rapporti records that will be orphaned when Collegamenti is deleted", rapportiToOrphan.size());
        
        // Get error types
        ErrorType orphanCollegamentiType = errorTypeRepository.findByErrorCode(ErrorTypeCode.ORPHAN_COLLEGAMENTI.getErrorCode())
                .orElseThrow(() -> new RuntimeException("Error type ORPHAN_COLLEGAMENTI not found in database"));
        ErrorType orphanSoggettiType = errorTypeRepository.findByErrorCode(ErrorTypeCode.ORPHAN_SOGGETTI.getErrorCode())
                .orElseThrow(() -> new RuntimeException("Error type ORPHAN_SOGGETTI not found in database"));
        ErrorType orphanRapportiType = errorTypeRepository.findByErrorCode(ErrorTypeCode.ORPHAN_RAPPORTI.getErrorCode())
                .orElseThrow(() -> new RuntimeException("Error type ORPHAN_RAPPORTI not found in database"));
        
        // Track IDs for deletion
        List<Long> collegamentiIds = new ArrayList<>();
        List<Long> soggettiIds = new ArrayList<>();
        List<Long> rapportiIds = new ArrayList<>();
        
        // Collect all error records (without saving to DB yet)
        List<ErrorRecord> allErrorRecords = new ArrayList<>();
        
        // Step 4: Create error record objects for orphan Collegamenti
        for (Object[]orphan : orphanCollegamenti) {
            Long collegamentiId = ((Number) orphan[0]).longValue();
            String ndg = (String) orphan[1];
            String chiaveRapporto = (String) orphan[2];
            Integer hasSoggetti = ((Number) orphan[3]).intValue();
            Integer hasRapporti = ((Number) orphan[4]).intValue();
            String rawRow = (String) orphan[5];
            
            collegamentiIds.add(collegamentiId);
            
            // Determine what's missing and create appropriate error message
            String errorMessage;
            if (hasSoggetti == 0 && hasRapporti == 0) {
                errorMessage = String.format(
                    "Collegamenti record (ID: %d, NDG: %s, chiave_rapporto: %s) is orphaned - missing both Soggetti and Rapporti children. " +
                    "This Collegamenti will be deleted to ensure equal counts (Collegamenti = Soggetti = Rapporti).",
                    collegamentiId, ndg, chiaveRapporto
                );
            } else if (hasSoggetti == 0) {
                errorMessage = String.format(
                    "Collegamenti record (ID: %d, NDG: %s, chiave_rapporto: %s) is missing Soggetti child. " +
                    "This Collegamenti and its Rapporti child will be deleted to ensure equal counts (Collegamenti = Soggetti = Rapporti).",
                    collegamentiId, ndg, chiaveRapporto
                );
            } else {
                errorMessage = String.format(
                    "Collegamenti record (ID: %d, NDG: %s, chiave_rapporto: %s) is missing Rapporti child. " +
                    "This Collegamenti and its Soggetti child will be deleted to ensure equal counts (Collegamenti = Soggetti = Rapporti).",
                    collegamentiId, ndg, chiaveRapporto
                );
            }
            
            ErrorRecord errorRecord = createErrorRecordObject(ingestion, submission, rawRow, orphanCollegamentiType, errorMessage);
            allErrorRecords.add(errorRecord);
            log.debug("Created error record object for orphan Collegamenti: {}", errorMessage);
        }
        
        // Step 5: Create error record objects for Soggetti that will be orphaned
        for (Object[] soggetto : soggettiToOrphan) {
            Long soggettiId = ((Number) soggetto[0]).longValue();
            String ndg = (String) soggetto[1];
            String rawRow = (String) soggetto[2];
            
            soggettiIds.add(soggettiId);
            
            String errorMessage = String.format(
                "Soggetti record (ID: %d, NDG: %s) will be orphaned because its Collegamenti parent (NDG: %s) has no children and will be deleted. " +
                "This Soggetti will be deleted to maintain referential integrity.",
                soggettiId, ndg, ndg
            );
            
            ErrorRecord errorRecord = createErrorRecordObject(ingestion, submission, rawRow, orphanSoggettiType, errorMessage);
            allErrorRecords.add(errorRecord);
            log.debug("Created error record object for orphaned Soggetti: {}", errorMessage);
        }
        
        // Step 6: Create error record objects for Rapporti that will be orphaned
        for (Object[] rapporto : rapportiToOrphan) {
            Long rapportiId = ((Number) rapporto[0]).longValue();
            String chiaveRapporto = (String) rapporto[1];
            String rawRow = (String) rapporto[2];
            
            rapportiIds.add(rapportiId);
            
            String errorMessage = String.format(
                "Rapporti record (ID: %d, chiave_rapporto: %s) will be orphaned because its Collegamenti parent (chiave_rapporto: %s) has no children and will be deleted. " +
                "This Rapporti will be deleted to maintain referential integrity.",
                rapportiId, chiaveRapporto, chiaveRapporto
            );
            
            ErrorRecord errorRecord = createErrorRecordObject(ingestion, submission, rawRow, orphanRapportiType, errorMessage);
            allErrorRecords.add(errorRecord);
            log.debug("Created error record object for orphaned Rapporti: {}", errorMessage);
        }
        
        // Step 7: Bulk insert all error records with their causes
        if (!allErrorRecords.isEmpty()) {
            log.info("Bulk inserting {} error records with causes", allErrorRecords.size());
            errorRecordRepository.bulkInsertRecordsWithCauses(allErrorRecords, ingestion.getId());
            log.info("Successfully bulk inserted {} error records", allErrorRecords.size());
        }
        
        // Step 8: Delete in correct order (children first, then parent) using BULK operations
        int soggettiDeletedCount = 0;
        int rapportiDeletedCount = 0;
        int collegamentiDeletedCount = 0;
        
        if (!soggettiIds.isEmpty()) {
            log.info("Bulk deleting {} orphaned Soggetti records", soggettiIds.size());
            soggettiDeletedCount = soggettiRepository.bulkDeleteByIds(soggettiIds);
            log.info("Successfully bulk deleted {} Soggetti records", soggettiDeletedCount);
        }
        
        if (!rapportiIds.isEmpty()) {
            log.info("Bulk deleting {} orphaned Rapporti records", rapportiIds.size());
            rapportiDeletedCount = rapportiRepository.bulkDeleteByIds(rapportiIds);
            log.info("Successfully bulk deleted {} Rapporti records", rapportiDeletedCount);
        }
        
        if (!collegamentiIds.isEmpty()) {
            log.info("Bulk deleting {} orphan Collegamenti records", collegamentiIds.size());
            collegamentiDeletedCount = collegamentiRepository.bulkDeleteByIds(collegamentiIds);
            log.info("Successfully bulk deleted {} Collegamenti records", collegamentiDeletedCount);
        }
        
        long elapsedTime = System.currentTimeMillis() - startTime;
        log.info("Orphan validation completed in {}ms - Deleted: {} Collegamenti, {} Soggetti, {} Rapporti (using bulk operations)",
                elapsedTime, collegamentiDeletedCount, soggettiDeletedCount, rapportiDeletedCount);
        
        return new OrphanValidationResult(collegamentiDeletedCount, soggettiDeletedCount, rapportiDeletedCount);
    }
    
    /**
     * Helper method to create error record object with error cause (without saving to DB).
     * Used for bulk insertion.
     */
    private ErrorRecord createErrorRecordObject(Ingestion ingestion, Submission submission, String rawRow, 
                                                ErrorType errorType, String errorMessage) {
        ErrorRecord errorRecord = new ErrorRecord();
        errorRecord.setIngestion(ingestion);
        errorRecord.setSubmission(submission);
        errorRecord.setRawRow(rawRow != null && rawRow.length() > 250 ? rawRow.substring(0, 250) : rawRow);
        
        ErrorCause errorCause = new ErrorCause();
        errorCause.setErrorRecord(errorRecord);
        errorCause.setErrorType(errorType);
        errorCause.setErrorMessage(errorMessage);
        errorCause.setSubmission(submission);
        
        // Set the error causes list on the error record
        errorRecord.setErrorCauses(List.of(errorCause));
        
        return errorRecord;
    }
    
    /**
     * Helper method to create error record and error cause.
     */
    private void createErrorRecord(Ingestion ingestion, Submission submission, String rawRow, 
                                   ErrorType errorType, String errorMessage) {
        ErrorRecord errorRecord = new ErrorRecord();
        errorRecord.setIngestion(ingestion);
        errorRecord.setSubmission(submission);
        errorRecord.setRawRow(rawRow != null && rawRow.length() > 250 ? rawRow.substring(0, 250) : rawRow);
        errorRecordRepository.save(errorRecord);
        
        ErrorCause errorCause = new ErrorCause();
        errorCause.setErrorRecord(errorRecord);
        errorCause.setErrorType(errorType);
        errorCause.setErrorMessage(errorMessage);
        errorCause.setSubmission(submission);
        errorCauseRepository.save(errorCause);
    }
    
    /**
     * Result of orphan validation containing counts of deleted records.
     */
    private record OrphanValidationResult(int collegamentiDeleted, int soggettiDeleted, int rapportiDeleted) {
        public int totalDeleted() {
            return collegamentiDeleted + soggettiDeleted + rapportiDeleted;
        }
    }

    private void cleanUpFailedSubmission(Submission submission, Ingestion ingestion, Exception exception) throws NotFoundRecordException {
        // Build comprehensive error message with full exception details
        StringBuilder fullErrorMsg = new StringBuilder();
        fullErrorMsg.append("Ingestion ID: ").append(ingestion.getId()).append(" failed\n");
        fullErrorMsg.append("Error Type: ").append(exception.getClass().getName()).append("\n");
        fullErrorMsg.append("Error Message: ").append(exception.getMessage()).append("\n");
        
        // Add cause if present
        Throwable cause = exception.getCause();
        if (cause != null) {
            fullErrorMsg.append("Caused by: ").append(cause.getClass().getName()).append("\n");
            fullErrorMsg.append("Cause Message: ").append(cause.getMessage()).append("\n");
        }
        
        // Add first few stack trace elements for context
        fullErrorMsg.append("Stack Trace:\n");
        StackTraceElement[] stackTrace = exception.getStackTrace();
        int maxStackLines = Math.min(10, stackTrace.length);
        for (int i = 0; i < maxStackLines; i++) {
            fullErrorMsg.append("  at ").append(stackTrace[i].toString()).append("\n");
        }
        
        String fullError = fullErrorMsg.toString();
        
        // Log the full error message
        log.error("=== FULL ERROR DETAILS FOR INGESTION {} ===", ingestion.getId());
        log.error(fullError);
        log.error("=== END ERROR DETAILS ===");
        
        // Save to Log table (now supports TEXT, no truncation needed)
        Log logEntry = new Log();
        logEntry.setSubmission(submission);
        logEntry.setBeforeSubmissionStatus(submission.getCurrentSubmissionStatus());
        logEntry.setMessage(fullError); // Full error message, no truncation

        submissionService.markAsError(submission);

        logEntry.setAfterSubmissionStatus(submission.getCurrentSubmissionStatus());
        logRepository.save(logEntry);

        // Delete in correct order: children before parent (respecting FK constraints)
        log.info("Cleaning up failed submission {} - deleting records in correct order", submission.getId());
        
        // Phase 1: Delete independent table
        int cambioNdgDeleted = cambioNdgRepository.deleteBySubmissionId(submission.getId());
        log.info("Deleted {} CambioNdg records", cambioNdgDeleted);
        
        // Phase 2: Delete children (have FK to Collegamenti)
        int datiContabiliDeleted = datiContabiliRepository.deleteBySubmissionId(submission.getId());
        log.info("Deleted {} DatiContabili records", datiContabiliDeleted);
        
        int rapportiDeleted = rapportiRepository.deleteBySubmissionId(submission.getId());
        log.info("Deleted {} Rapporti records", rapportiDeleted);
        
        int soggettiDeleted = soggettiRepository.deleteBySubmissionId(submission.getId());
        log.info("Deleted {} Soggetti records", soggettiDeleted);
        
        // Phase 3: Delete parent (must be last)
        int collegamentiDeleted = collegamentiRepository.deleteBySubmissionId(submission.getId());
        log.info("Deleted {} Collegamenti records", collegamentiDeleted);
        
        // Phase 4: Delete error tracking
        int errorCauseDeleted = errorCauseRepository.deleteBySubmissionId(submission.getId());
        log.info("Deleted {} ErrorCause records", errorCauseDeleted);
        
        int errorRecordDeleted = errorRecordRepository.deleteBySubmissionId(submission.getId());
        log.info("Deleted {} ErrorRecord records", errorRecordDeleted);

        IngestionStatus errorStatus = ingestionStatusRepository.findOneByName(IngestionStatusEnum.FAILED.name())
                .orElseThrow(() -> new NotFoundRecordException("Ingestion status with name failed is not found"));

        IngestionError ingestionError = new IngestionError();
        // Full error message, no truncation (TEXT column supports it)
        ingestionError.setDescription(fullError);
        ingestionErrorRepository.save(ingestionError);

        ingestion.setIngestionStatus(errorStatus);
        ingestion.setIngestionError(ingestionError);
        ingestionRepository.save(ingestion);
        
        log.info("Cleanup completed for failed submission {}", submission.getId());
    }

//    private void retryFailedTransactions(Obligation obligation, Period period, Submission currentSubmission) {
//        int periodOrder = period.getOrder();
//        int year = obligation.getFiscalYear();
//        if (periodOrder == 1) {
//            year--;
//            periodOrder = 12;
//        } else {
//            periodOrder--;
//        }
//        Period lastPeriod = periodRepository.findByOrder(periodOrder).orElse(null);
//
//        if (lastPeriod == null) return;
//
//        Obligation oldObligation = obligationRepository.findByFiscalYearAndPeriod(year, lastPeriod).orElse(null);
//        if (oldObligation == null) return;
//        oldObligation.setPeriod(lastPeriod);
//
//        List<Submission> submissions = submissionRepository.findByObligationId(oldObligation.getId());
//        Submission activeSubmission = submissions.stream()
//                .filter(s ->
//                        !s.getCurrentSubmissionStatus().getName().equals(SubmissionStatusEnum.CANCELLED.getDbName())
//                                && !s.getCurrentSubmissionStatus().getName().equals(SubmissionStatusEnum.REJECTED.getDbName())
//                )
//                .max(Comparator.comparing(Submission::getLastUpdatedAt))
//                .orElse(null);
//
//        if (activeSubmission == null) return;
//
//        Ingestion ingestion = ingestionRepository.findFirstBySubmission_IdAndIngestionType_Name(
//                activeSubmission.getId(), IngestionTypeEnum.TRANSACTIONS.getLabel()).orElse(null);
//
//        if (ingestion == null) return;
//
//        List<ErrorRecord> failedTransactions = errorRecordRepository.findRecordsWithExactlyOneCauseOfType(
//                ingestion.getId(), ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode());
//
//        List<String> rawRows = failedTransactions.stream()
//                .map(ErrorRecord::getRawRow)
//                .toList();
//
//        if (rawRows.isEmpty()) return;
//
//        List<ProcessedFailedRecordBatch> processedBatches = transactionFileProcessingService.processFailedTransactions(
//                rawRows, ingestion, activeSubmission, oldObligation);
//
//        for (ProcessedFailedRecordBatch batch : processedBatches) {
//            if (!batch.transactions().isEmpty()) {
//                resolvedTransactionRepository.bulkInsert(batch.transactions(), currentSubmission, activeSubmission);
//
//                Set<String> resolvedRawRows = batch.transactions().stream()
//                        .map(ResolvedTransaction::getRawRow)
//                        .filter(Objects::nonNull)
//                        .collect(Collectors.toSet());
//
//                if (!resolvedRawRows.isEmpty()) {
//                    List<ErrorRecord> toDelete = failedTransactions.stream()
//                            .filter(er -> er.getRawRow() != null && resolvedRawRows.contains(er.getRawRow()))
//                            .toList();
//
//                    if (!toDelete.isEmpty()) {
//                        log.info("Deleting {} resolved error record(s) from ingestion {}", toDelete.size(), ingestion.getId());
//                        errorRecordRepository.deleteAll(toDelete);
//                    }
//                }
//            }
//        }
//    }
}