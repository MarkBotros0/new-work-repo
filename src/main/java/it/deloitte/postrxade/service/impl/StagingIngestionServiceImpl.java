package it.deloitte.postrxade.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import it.deloitte.postrxade.entity.DatiContabili;
import it.deloitte.postrxade.entity.ErrorCause;
import it.deloitte.postrxade.entity.ErrorRecord;
import it.deloitte.postrxade.entity.ErrorType;
import it.deloitte.postrxade.entity.Ingestion;
import it.deloitte.postrxade.entity.Soggetti;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.enums.ErrorTypeCode;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.parser.merchants.MerchantFileLineParser;
import it.deloitte.postrxade.parser.merchants.MerchantFileLineValidator;
import it.deloitte.postrxade.parser.merchants.types.CambioNdgRecord;
import it.deloitte.postrxade.parser.merchants.types.DatiContabiliRecord;
import it.deloitte.postrxade.parser.merchants.types.SoggettiRecord;
import it.deloitte.postrxade.parser.transaction.RemoteFile;
import it.deloitte.postrxade.records.ErrorRecordCause;
import it.deloitte.postrxade.records.StagingResult;
import it.deloitte.postrxade.repository.ErrorRecordRepository;
import it.deloitte.postrxade.repository.StagingRepository;
import it.deloitte.postrxade.service.ErrorTypeService;
import it.deloitte.postrxade.service.StagingIngestionService;
import lombok.extern.slf4j.Slf4j;
import ma.glasnost.orika.MapperFacade;

/**
 * Implementation of staging-based ingestion service.
 *
 * <p>This implementation provides performance improvements over row-by-row processing
 * while maintaining ALL business validation rules:</p>
 *
 * <ul>
 *   <li>Full data quality validation (format, mandatory fields, values)</li>
 *   <li>Intra-file duplicate detection for merchants</li>
 *   <li>Database duplicate detection via staging tables</li>
 *   <li>Missing merchant detection for transactions</li>
 *   <li>ErrorRecord/ErrorCause creation for all validation failures</li>
 * </ul>
 */
@Service
@Slf4j
public class StagingIngestionServiceImpl implements StagingIngestionService {

    private final StagingRepository stagingRepository;
    private final ErrorRecordRepository errorRecordRepository;
    private final ErrorTypeService errorTypeService;
    private final MapperFacade mapperFacade;
    private final MerchantFileLineParser parser = new MerchantFileLineParser();
    private final MerchantFileLineValidator validator = new MerchantFileLineValidator();

    public StagingIngestionServiceImpl(
            StagingRepository stagingRepository,
            ErrorRecordRepository errorRecordRepository,
            ErrorTypeService errorTypeService,
            @Qualifier("mapperFacade") MapperFacade mapperFacade) {
        this.stagingRepository = stagingRepository;
        this.errorRecordRepository = errorRecordRepository;
        this.errorTypeService = errorTypeService;
        this.mapperFacade = mapperFacade;
    }

    // ==================== MERCHANT FILE PROCESSING ====================

    // Batch size for streaming - process merchants in chunks to avoid OOM
    private static final int MERCHANT_PARSE_BATCH_SIZE = 50000;

    @Override
    public StagingResult processSoggettiFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException {
        log.info("Starting STREAMING staging-based merchant ingestion for file: {}, submission: {}",
                file.name(), submission.getId());
        long startTime = System.currentTimeMillis();

        // Phase 1: Read all lines as strings (low memory footprint)
        List<String> lines = readAllLines(file);
        if (lines.isEmpty()) {
            return new StagingResult(0, 0);
        }

        int totalLines = lines.size();
        log.info("Read {} lines from file in {}ms", totalLines, System.currentTimeMillis() - startTime);

        // Phase 2: Process in BATCHES to avoid OOM
        int totalValidationErrors = 0;
        int totalParsed = 0;
        int batchNumber = 0;

        List<Soggetti> batchSoggetti = new ArrayList<>(MERCHANT_PARSE_BATCH_SIZE);
        List<ErrorRecord> batchErrors = new ArrayList<>();
        Set<String> fileLevelMerchants = new HashSet<>(); // For intra-file duplicate detection (kept across batches)

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            try {
                ErrorRecord error = processSoggettiLine(line, ingestion, submission, batchSoggetti, fileLevelMerchants);
                if (error != null) {
                    batchErrors.add(error);
                }
            } catch (Exception e) {
                if (totalValidationErrors < 10) {
                    log.warn("Error processing merchant line {}: {}", i + 1, e.getMessage());
                }
                try {
                    ErrorRecord error = createErrorRecordFromException(e, line, ingestion, submission);
                    batchErrors.add(error);
                } catch (NotFoundRecordException ex) {
                    log.error("Failed to create error record for exception", ex);
                }
            }

            // When batch is full OR at end of file, load to staging
            if (batchSoggetti.size() >= MERCHANT_PARSE_BATCH_SIZE || i == lines.size() - 1) {
                if (!batchSoggetti.isEmpty() || !batchErrors.isEmpty()) {
                    batchNumber++;
                    log.info("Processing batch {}: {} merchants, {} errors (total parsed: {}/{})",
                            batchNumber, batchSoggetti.size(), batchErrors.size(),
                            i + 1, lines.size());

                    // Save errors for this batch
                    if (!batchErrors.isEmpty()) {
                        errorRecordRepository.bulkInsertRecordsWithCauses(batchErrors, ingestion.getId());
                        totalValidationErrors += batchErrors.size();
                    }

                    // Load merchants to staging (NOT final insert yet)
                    if (!batchSoggetti.isEmpty()) {
                        stagingRepository.bulkLoadSoggettiToStaging(batchSoggetti, ingestion.getId(), submission.getId());
                        totalParsed += batchSoggetti.size();
                    }

                    // CRITICAL: Clear batch lists to free memory (but keep fileLevelMerchants for dedup)
                    batchSoggetti.clear();
                    batchErrors.clear();
                }
            }
        }

        // Clear original lines list to free memory before final processing
        lines.clear();

        log.info("Loaded {} merchants to staging in {} batches, {} validation errors, in {}ms",
                totalParsed, batchNumber, totalValidationErrors, System.currentTimeMillis() - startTime);

        // Phase 3: Set-based processing (duplicate detection, final insert)
        log.info("Starting set-based processing from staging...");
        long processStart = System.currentTimeMillis();
        StagingResult stagingResult = stagingRepository.processSoggettiFromStaging(submission.getId());
        log.info("Set-based processing completed in {}ms", System.currentTimeMillis() - processStart);

        // Phase 4: Create ErrorRecords for DB duplicates
        if (stagingResult.duplicateCount() > 0) {
            createErrorRecordsForDuplicateSoggetti(ingestion, submission);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Completed merchant ingestion in {}ms: parsed={}, inserted={}, duplicates={}, validationErrors={}",
                elapsed, totalParsed, stagingResult.insertedCount(), stagingResult.duplicateCount(), totalValidationErrors);

        return new StagingResult(stagingResult.insertedCount(), stagingResult.duplicateCount(), 0, totalValidationErrors);
    }

    /**
     * Process a single merchant line with full validation (same logic as FileProcessingService).
     */
    private ErrorRecord processSoggettiLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<Soggetti> soggettiList,
            Set<String> fileLevelSoggetti) throws NotFoundRecordException {

        SoggettiRecord record = parser.parseSoggettiLine(line);

        // Validate with all business rules (format, mandatory fields, values)
        List<ErrorRecordCause> errorCauses = validator.validateSoggetti(record, fileLevelSoggetti);

        // Check for intra-file duplicates
        String merchantRecordKey = record.getIntermediario() + "_" + record.getNdg() + "_" + submission.getId();
        if (fileLevelSoggetti.contains(merchantRecordKey)) {
            errorCauses.add(new ErrorRecordCause(
                    "Soggetti already exists in the same file",
                    ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode()
            ));
        }

        if (errorCauses.isEmpty()) {
            // Valid record - add to list for staging
            Soggetti soggetti = mapperFacade.map(record, Soggetti.class);
            soggetti.setIngestion(ingestion);
            soggetti.setSubmission(submission);
            soggetti.setRawRow(line);
            soggettiList.add(soggetti);
            fileLevelSoggetti.add(merchantRecordKey);
            return null;
        } else {
            // Invalid record - create error
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
    }

    /**
     * Process a single rapporti line with full validation.
     */
    private ErrorRecord processRapportiLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<it.deloitte.postrxade.entity.Rapporti> rapportiList,
            Set<String> fileLevelRapporti) throws NotFoundRecordException {

        it.deloitte.postrxade.parser.merchants.types.RapportiRecord record = parser.parseRapportoLine(line);

        // Validate with all business rules (format, mandatory fields, values)
        List<ErrorRecordCause> errorCauses = validator.validateRapporto(record);

        // Check for intra-file duplicates
        String recordKey = record.getIntermediario() + "_" + record.getChiaveRapporto() + "_" + submission.getId();
        if (fileLevelRapporti.contains(recordKey)) {
            errorCauses.add(new ErrorRecordCause(
                    "Record already exists in the same file",
                    ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode()
            ));
        }

        if (errorCauses.isEmpty()) {
            // Valid record - add to list for staging
            it.deloitte.postrxade.entity.Rapporti entity = mapperFacade.map(record, it.deloitte.postrxade.entity.Rapporti.class);
            entity.setIngestion(ingestion);
            entity.setSubmission(submission);
            entity.setRawRow(line);
            rapportiList.add(entity);
            fileLevelRapporti.add(recordKey);
            return null;
        } else {
            // Invalid record - create error
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
    }

    /**
     * Process a single daticontabili line with full validation.
     */
    private ErrorRecord processDaticontabiliLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<it.deloitte.postrxade.entity.DatiContabili> daticontabiliList,
            Set<String> fileLevelDaticontabili) throws NotFoundRecordException {

        DatiContabiliRecord record = parser.parseDatiContabiliLine(line);

        // Validate with all business rules (format, mandatory fields, values)
        List<ErrorRecordCause> errorCauses = validator.validateDatiContabili(record, fileLevelDaticontabili);

        // Check for intra-file duplicates
        String recordKey = record.getIntermediario() + "_" + record.getChiaveRapporto() + "_" + submission.getId();
        if (fileLevelDaticontabili.contains(recordKey)) {
            errorCauses.add(new ErrorRecordCause(
                    "Record already exists in the same file",
                    ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode()
            ));
        }

        if (errorCauses.isEmpty()) {
            // Valid record - add to list for staging
            DatiContabili entity = mapperFacade.map(record, it.deloitte.postrxade.entity.DatiContabili.class);
            entity.setIngestion(ingestion);
            entity.setSubmission(submission);
            entity.setRawRow(line);
            daticontabiliList.add(entity);
            fileLevelDaticontabili.add(recordKey);
            return null;
        } else {
            // Invalid record - create error
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
    }

    /**
     * Process a single collegamenti line with full validation.
     */
    private ErrorRecord processCollegamentiLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<it.deloitte.postrxade.entity.Collegamenti> collegamentiList,
            Set<String> fileLevelCollegamenti) throws NotFoundRecordException {

        it.deloitte.postrxade.parser.merchants.types.CollegamentiRecord record = parser.parseCollegamentiLine(line);

        // Validate with all business rules (format, mandatory fields, values)
        List<ErrorRecordCause> errorCauses = validator.validateCollegamenti(record, fileLevelCollegamenti);

        // Check for intra-file duplicates
        String recordKey = record.getIntermediario() + "_" + record.getChiaveRapporto() + "_" + record.getNdg() + "_" + submission.getId();

        if (fileLevelCollegamenti.contains(recordKey)) {
            errorCauses.add(new ErrorRecordCause(
                    "Record already exists in the same file",
                    ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode()
            ));
        }

        if (errorCauses.isEmpty()) {
            // Valid record - add to list for staging
            it.deloitte.postrxade.entity.Collegamenti entity = mapperFacade.map(record, it.deloitte.postrxade.entity.Collegamenti.class);
            entity.setIngestion(ingestion);
            entity.setSubmission(submission);
            entity.setRawRow(line);
            collegamentiList.add(entity);
            fileLevelCollegamenti.add(recordKey);
            return null;
        } else {
            // Invalid record - create error
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
    }

    /**
     * Process a single collegamenti line with full validation.
     */
    private ErrorRecord processCambioNdgLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<it.deloitte.postrxade.entity.CambioNdg> cambioNdgList,
            Set<String> fileLevelCambioNdg) throws NotFoundRecordException {

        CambioNdgRecord record = parser.parseCambioNdgLine(line);

        // Validate with all business rules (format, mandatory fields, values)
        List<ErrorRecordCause> errorCauses = validator.validateCambioNdg(record, fileLevelCambioNdg);

        // Check for intra-file duplicates
        String recordKey = record.getIntermediario() + "_" + record.getNdgVecchio() + "_" + record.getNdgVecchio() + "_" + submission.getId();
        if (fileLevelCambioNdg.contains(recordKey)) {
            errorCauses.add(new ErrorRecordCause(
                    "Record already exists in the same file",
                    ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode()
            ));
        }

        if (errorCauses.isEmpty()) {
            // Valid record - add to list for staging
            it.deloitte.postrxade.entity.CambioNdg entity = mapperFacade.map(record, it.deloitte.postrxade.entity.CambioNdg.class);
            entity.setIngestion(ingestion);
            entity.setSubmission(submission);
            entity.setRawRow(line);
            cambioNdgList.add(entity);
            fileLevelCambioNdg.add(recordKey);
            return null;
        } else {
            // Invalid record - create error
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
    }

    // ==================== TRANSACTION FILE PROCESSING ====================

    // Batch size for streaming - process transactions in chunks to avoid OOM
    // Reduced from 50000 to 10000 for very large files (11M+ records) to prevent memory issues
    // Each Transaction object is ~500-1000 bytes, so 10000 = ~5-10MB per batch (safe)
    private static final int TRANSACTION_PARSE_BATCH_SIZE = 10000;

    // ==================== STAGING TABLE PROCESSING ====================

//    @Override
//    @Transactional
//    public StagingResult processSoggetti(List<Soggetti> soggettiList, List<ErrorRecord> validationErrors,
//                                         Ingestion ingestion, Submission submission) {
//        // Save validation errors first
//        if (!validationErrors.isEmpty()) {
//            log.info("Saving {} validation error records for merchants", validationErrors.size());
//            errorRecordRepository.bulkInsertRecordsWithCauses(validationErrors, ingestion.getId());
//        }
//
//        if (soggettiList == null || soggettiList.isEmpty()) {
//            return new StagingResult(0, 0, 0, validationErrors.size());
//        }
//
//        log.info("Processing {} valid soggetti via staging tables", soggettiList.size());
//        long startTime = System.currentTimeMillis();
//
//        // Phase 1: Bulk load into staging table
//        long loadStart = System.currentTimeMillis();
//        stagingRepository.bulkLoadSoggettiToStaging(soggettiList, ingestion.getId(), submission.getId());
//        log.info("Load phase completed in {}ms", System.currentTimeMillis() - loadStart);
//
//        // Phase 2: Set-based processing (DB duplicate detection + insert)
//        long processStart = System.currentTimeMillis();
//        StagingResult result = stagingRepository.processSoggettiFromStaging(submission.getId());
//        log.info("Process phase completed in {}ms", System.currentTimeMillis() - processStart);
//
//        // Phase 3: Create ErrorRecords for DB duplicates
//        if (result.duplicateCount() > 0) {
//            long errorStart = System.currentTimeMillis();
//            int errorCount = createErrorRecordsForDuplicateSoggetti(ingestion, submission);
//            log.info("Created {} error records for DB duplicate merchants in {}ms",
//                    errorCount, System.currentTimeMillis() - errorStart);
//        }
//
//        long elapsed = System.currentTimeMillis() - startTime;
//        log.info("Merchant staging processing completed in {}ms: inserted={}, duplicates={}",
//                elapsed, result.insertedCount(), result.duplicateCount());
//
//        return new StagingResult(result.insertedCount(), result.duplicateCount(), 0, validationErrors.size());
//    }

    @Override
    @Transactional
    public void cleanupStaging(Long submissionId) {
        log.info("Cleaning up staging tables for submission: {}", submissionId);
        stagingRepository.clearStaging(submissionId);
    }

//    @Override
//    @Transactional
//    public void cleanupTransactionStaging(Long submissionId) {
//        log.info("Cleaning up transaction staging table for submission: {}", submissionId);
//        stagingRepository.clearTransactionStaging(submissionId);
//    }

    // ==================== ERROR RECORD CREATION ====================

    /**
     * Create ErrorRecords for duplicate merchants found in staging (DB duplicates).
     */
//    private int createErrorRecordsForDuplicateMerchants(Ingestion ingestion, Submission submission) {
//        List<Object[]> duplicates = stagingRepository.getDuplicateMerchantDetails(submission.getId());
//        if (duplicates.isEmpty()) {
//            return 0;
//        }
//
//        List<ErrorRecord> errorRecords = new ArrayList<>();
//        for (Object[] row : duplicates) {
//            String rawRow = row[0] != null ? String.valueOf(row[0]) : "";
//            String errorMessage = row[1] != null ? String.valueOf(row[1]) : "Merchant already exists in database";
//
//            try {
//                ErrorRecord errorRecord = createErrorRecord(
//                        List.of(new ErrorRecordCause(errorMessage, ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())),
//                        rawRow, ingestion, submission
//                );
//                errorRecords.add(errorRecord);
//            } catch (NotFoundRecordException e) {
//                log.warn("Failed to create error record for duplicate merchant: {}", e.getMessage());
//            }
//        }
//
//        if (!errorRecords.isEmpty()) {
//            errorRecordRepository.bulkInsertRecordsWithCauses(errorRecords, ingestion.getId());
//        }
//        return errorRecords.size();
//    }
    private int createErrorRecordsForDuplicateSoggetti(Ingestion ingestion, Submission submission) {
        List<Object[]> duplicates = stagingRepository.getDuplicateSoggettiDetails(submission.getId());
        if (duplicates.isEmpty()) return 0;

        List<ErrorRecord> errorRecords = new ArrayList<>();
        for (Object[] row : duplicates) {
            String rawRow = row[0] != null ? String.valueOf(row[0]) : "";
            String errorMessage = row[1] != null ? String.valueOf(row[1]) : "Soggetto already exists";

            try {
                ErrorRecord errorRecord = createErrorRecord(
                        List.of(new ErrorRecordCause(errorMessage, ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())),
                        rawRow, ingestion, submission
                );
                errorRecords.add(errorRecord);
            } catch (NotFoundRecordException e) {
                log.warn("Failed to create error record for duplicate soggetti: {}", e.getMessage());
            }
        }

        if (!errorRecords.isEmpty()) {
            errorRecordRepository.bulkInsertRecordsWithCauses(errorRecords, ingestion.getId());
        }
        return errorRecords.size();
    }

    private int createErrorRecordsForDuplicateRapporti(Ingestion ingestion, Submission submission) {
        List<Object[]> duplicates = stagingRepository.getDuplicateRapportiDetails(submission.getId());
        if (duplicates.isEmpty()) return 0;

        List<ErrorRecord> errorRecords = new ArrayList<>();
        for (Object[] row : duplicates) {
            String rawRow = row[0] != null ? String.valueOf(row[0]) : "";
            String errorMessage = row[1] != null ? String.valueOf(row[1]) : "Rapporto already exists";

            try {
                ErrorRecord errorRecord = createErrorRecord(
                        List.of(new ErrorRecordCause(errorMessage, ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())),
                        rawRow, ingestion, submission
                );
                errorRecords.add(errorRecord);
            } catch (NotFoundRecordException e) {
                log.warn("Failed to create error record for duplicate rapporti: {}", e.getMessage());
            }
        }

        if (!errorRecords.isEmpty()) {
            errorRecordRepository.bulkInsertRecordsWithCauses(errorRecords, ingestion.getId());
        }
        return errorRecords.size();
    }

    private int createErrorRecordsForDuplicateDatiContabili(Ingestion ingestion, Submission submission) {
        List<Object[]> duplicates = stagingRepository.getDuplicateDatiContabiliDetails(submission.getId());
        if (duplicates.isEmpty()) return 0;

        List<ErrorRecord> errorRecords = new ArrayList<>();
        for (Object[] row : duplicates) {
            String rawRow = row[0] != null ? String.valueOf(row[0]) : "";
            String errorMessage = row[1] != null ? String.valueOf(row[1]) : "Dati Contabili record already exists";

            try {
                ErrorRecord errorRecord = createErrorRecord(
                        List.of(new ErrorRecordCause(errorMessage, ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())),
                        rawRow, ingestion, submission
                );
                errorRecords.add(errorRecord);
            } catch (NotFoundRecordException e) {
                log.warn("Failed to create error record for duplicate dati contabili: {}", e.getMessage());
            }
        }

        if (!errorRecords.isEmpty()) {
            errorRecordRepository.bulkInsertRecordsWithCauses(errorRecords, ingestion.getId());
        }
        return errorRecords.size();
    }

    private int createErrorRecordsForDuplicateCollegamenti(Ingestion ingestion, Submission submission) {
        List<Object[]> duplicates = stagingRepository.getDuplicateCollegamentiDetails(submission.getId());
        if (duplicates.isEmpty()) return 0;

        List<ErrorRecord> errorRecords = new ArrayList<>();
        for (Object[] row : duplicates) {
            String rawRow = row[0] != null ? String.valueOf(row[0]) : "";
            String errorMessage = row[1] != null ? String.valueOf(row[1]) : "Collegamento already exists";

            try {
                ErrorRecord errorRecord = createErrorRecord(
                        List.of(new ErrorRecordCause(errorMessage, ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())),
                        rawRow, ingestion, submission
                );
                errorRecords.add(errorRecord);
            } catch (NotFoundRecordException e) {
                log.warn("Failed to create error record for duplicate collegamenti: {}", e.getMessage());
            }
        }

        if (!errorRecords.isEmpty()) {
            errorRecordRepository.bulkInsertRecordsWithCauses(errorRecords, ingestion.getId());
        }
        return errorRecords.size();
    }

    private int createErrorRecordsForMissingReferenceCollegamenti(Ingestion ingestion, Submission submission) {
        List<Object[]> missingReferences = stagingRepository.getMissingReferenceCollegamentiDetails(submission.getId());
        if (missingReferences.isEmpty()) return 0;

        List<ErrorRecord> errorRecords = new ArrayList<>();
        for (Object[] row : missingReferences) {
            String rawRow = row[0] != null ? String.valueOf(row[0]) : "";
            String errorMessage = row[1] != null ? String.valueOf(row[1]) : "Foreign key violation: ndg not found in MERCHANT_SOGGETTI";

            try {
                ErrorRecord errorRecord = createErrorRecord(
                        List.of(new ErrorRecordCause(errorMessage, ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode())),
                        rawRow, ingestion, submission
                );
                errorRecords.add(errorRecord);
            } catch (NotFoundRecordException e) {
                log.warn("Failed to create error record for missing reference in collegamenti: {}", e.getMessage());
            }
        }

        if (!errorRecords.isEmpty()) {
            errorRecordRepository.bulkInsertRecordsWithCauses(errorRecords, ingestion.getId());
        }
        return errorRecords.size();
    }

    private int createErrorRecordsForDuplicateCambioNdg(Ingestion ingestion, Submission submission) {
        List<Object[]> duplicates = stagingRepository.getDuplicateCambioNdgDetails(submission.getId());
        if (duplicates.isEmpty()) return 0;

        List<ErrorRecord> errorRecords = new ArrayList<>();
        for (Object[] row : duplicates) {
            String rawRow = row[0] != null ? String.valueOf(row[0]) : "";
            String errorMessage = row[1] != null ? String.valueOf(row[1]) : "Cambio NDG record already exists";

            try {
                ErrorRecord errorRecord = createErrorRecord(
                        List.of(new ErrorRecordCause(errorMessage, ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())),
                        rawRow, ingestion, submission
                );
                errorRecords.add(errorRecord);
            } catch (NotFoundRecordException e) {
                log.warn("Failed to create error record for duplicate cambio ndg: {}", e.getMessage());
            }
        }

        if (!errorRecords.isEmpty()) {
            errorRecordRepository.bulkInsertRecordsWithCauses(errorRecords, ingestion.getId());
        }
        return errorRecords.size();
    }

    /**
     * Create ErrorRecords for duplicate transactions found in staging (DB duplicates).
     */
//    private int createErrorRecordsForDuplicateTransactions(Ingestion ingestion, Submission submission) {
//        List<Object[]> duplicates = stagingRepository.getDuplicateTransactionDetails(submission.getId());
//        if (duplicates.isEmpty()) {
//            return 0;
//        }
//
//        List<ErrorRecord> errorRecords = new ArrayList<>();
//        for (Object[] row : duplicates) {
//            String rawRow = row[0] != null ? String.valueOf(row[0]) : "";
//            String errorMessage = row[1] != null ? String.valueOf(row[1]) : "Transaction already exists in database";
//
//            try {
//                ErrorRecord errorRecord = createErrorRecord(
//                        List.of(new ErrorRecordCause(errorMessage, ErrorTypeCode.TRANSACTION_ALREADY_EXISTS.getErrorCode())),
//                        rawRow, ingestion, submission
//                );
//                errorRecords.add(errorRecord);
//            } catch (NotFoundRecordException e) {
//                log.warn("Failed to create error record for duplicate transaction: {}", e.getMessage());
//            }
//        }
//
//        if (!errorRecords.isEmpty()) {
//            errorRecordRepository.bulkInsertRecordsWithCauses(errorRecords, ingestion.getId());
//        }
//        return errorRecords.size();
//    }

    /**
     * Create ErrorRecords for transactions with missing merchants.
     */
//    private int createErrorRecordsForMissingMerchantTransactions(Ingestion ingestion, Submission submission) {
//        List<Object[]> missingMerchants = stagingRepository.getMissingMerchantTransactionDetails(submission.getId());
//        if (missingMerchants.isEmpty()) {
//            return 0;
//        }
//
//        List<ErrorRecord> errorRecords = new ArrayList<>();
//        for (Object[] row : missingMerchants) {
//            String rawRow = row[0] != null ? String.valueOf(row[0]) : "";
//            String errorMessage = row[1] != null ? String.valueOf(row[1]) : "Merchant not found for transaction";
//
//            try {
//                ErrorRecord errorRecord = createErrorRecord(
//                        List.of(new ErrorRecordCause(errorMessage, ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode())),
//                        rawRow, ingestion, submission
//                );
//                errorRecords.add(errorRecord);
//            } catch (NotFoundRecordException e) {
//                log.warn("Failed to create error record for missing merchant: {}", e.getMessage());
//            }
//        }
//
//        if (!errorRecords.isEmpty()) {
//            errorRecordRepository.bulkInsertRecordsWithCauses(errorRecords, ingestion.getId());
//        }
//        return errorRecords.size();
//    }
    @Override
    public StagingResult processRapportiFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException {
        log.info("Starting STREAMING staging-based rapporti ingestion for file: {}, submission: {}",
                file.name(), submission.getId());
        long startTime = System.currentTimeMillis();

        List<String> lines = readAllLines(file);
        if (lines.isEmpty()) {
            return new StagingResult(0, 0);
        }

        int totalLines = lines.size();
        log.info("Read {} lines from file in {}ms", totalLines, System.currentTimeMillis() - startTime);

        // Validate header/footer
//        validator.validateHeader(lines.getFirst(), ingestion.getIngestionType().getName());
//        validator.validateFooter(lines.getLast(), lines.size(), ingestion.getIngestionType().getName());
//        lines.removeFirst();
//        lines.removeLast();

        // Phase 2: Process in BATCHES to avoid OOM
        int totalValidationErrors = 0;
        int totalParsed = 0;
        int batchNumber = 0;

        List<it.deloitte.postrxade.entity.Rapporti> batchRapporti = new ArrayList<>();
        List<ErrorRecord> batchErrors = new ArrayList<>();
        Set<String> fileLevelRapporti = new HashSet<>();

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            try {
                ErrorRecord error = processRapportiLine(line, ingestion, submission, batchRapporti, fileLevelRapporti);
                if (error != null) {
                    batchErrors.add(error);
                }
            } catch (Exception e) {
                if (totalValidationErrors < 10) {
                    log.warn("Error processing rapporti line {}: {}", i + 1, e.getMessage());
                }
                try {
                    ErrorRecord error = createErrorRecordFromException(e, line, ingestion, submission);
                    batchErrors.add(error);
                } catch (NotFoundRecordException ex) {
                    log.error("Failed to create error record for exception", ex);
                }
            }

            if (batchRapporti.size() >= MERCHANT_PARSE_BATCH_SIZE || i == lines.size() - 1) {
                if (!batchRapporti.isEmpty() || !batchErrors.isEmpty()) {
                    batchNumber++;
                    log.info("Processing batch {}: {} rapporti, {} errors (total parsed: {}/{})",
                            batchNumber, batchRapporti.size(), batchErrors.size(),
                            i + 1, lines.size());

                    if (!batchErrors.isEmpty()) {
                        errorRecordRepository.bulkInsertRecordsWithCauses(batchErrors, ingestion.getId());
                        totalValidationErrors += batchErrors.size();
                    }

                    if (!batchRapporti.isEmpty()) {
                        stagingRepository.bulkLoadRapportiToStaging(batchRapporti, ingestion.getId(), submission.getId());
                        totalParsed += batchRapporti.size();
                    }

                    batchRapporti.clear();
                    batchErrors.clear();
                }
            }
        }

        lines.clear();

        log.info("Loaded {} rapporti to staging in {} batches, {} validation errors, in {}ms",
                totalParsed, batchNumber, totalValidationErrors, System.currentTimeMillis() - startTime);

        long processStart = System.currentTimeMillis();
        StagingResult stagingResult = stagingRepository.processRapportiFromStaging(submission.getId());
        log.info("Set-based processing completed in {}ms", System.currentTimeMillis() - processStart);

        if (stagingResult.duplicateCount() > 0) {
            createErrorRecordsForDuplicateRapporti(ingestion, submission);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Completed rapporti ingestion in {}ms: parsed={}, inserted={}, duplicates={}, validationErrors={}",
                elapsed, totalParsed, stagingResult.insertedCount(), stagingResult.duplicateCount(), totalValidationErrors);

        return new StagingResult(stagingResult.insertedCount(), stagingResult.duplicateCount(), 0, totalValidationErrors);
    }

    @Override
    public StagingResult processDaticontabiliFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException {
        log.info("Starting STREAMING staging-based daticontabili ingestion for file: {}, submission: {}",
                file.name(), submission.getId());
        long startTime = System.currentTimeMillis();

        List<String> lines = readAllLines(file);
        if (lines.isEmpty()) {
            return new StagingResult(0, 0);
        }

        int totalLines = lines.size();
        log.info("Read {} lines from file in {}ms", totalLines, System.currentTimeMillis() - startTime);

        // Validate header/footer
//        validator.validateHeader(lines.getFirst(), ingestion.getIngestionType().getName());
//        validator.validateFooter(lines.getLast(), lines.size(), ingestion.getIngestionType().getName());
//        lines.removeFirst();
//        lines.removeLast();

        // Phase 2: Process in BATCHES to avoid OOM
        int totalValidationErrors = 0;
        int totalParsed = 0;
        int batchNumber = 0;

        List<it.deloitte.postrxade.entity.DatiContabili> batchDaticontabili = new ArrayList<>();
        List<ErrorRecord> batchErrors = new ArrayList<>();
        Set<String> fileLevelDaticontabili = new HashSet<>();

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            try {
                ErrorRecord error = processDaticontabiliLine(line, ingestion, submission, batchDaticontabili, fileLevelDaticontabili);
                if (error != null) {
                    batchErrors.add(error);
                }
            } catch (Exception e) {
                if (totalValidationErrors < 10) {
                    log.warn("Error processing daticontabili line {}: {}", i + 1, e.getMessage());
                }
                try {
                    ErrorRecord error = createErrorRecordFromException(e, line, ingestion, submission);
                    batchErrors.add(error);
                } catch (NotFoundRecordException ex) {
                    log.error("Failed to create error record for exception", ex);
                }
            }

            if (batchDaticontabili.size() >= MERCHANT_PARSE_BATCH_SIZE || i == lines.size() - 1) {
                if (!batchDaticontabili.isEmpty() || !batchErrors.isEmpty()) {
                    batchNumber++;
                    log.info("Processing batch {}: {} daticontabili, {} errors (total parsed: {}/{})",
                            batchNumber, batchDaticontabili.size(), batchErrors.size(),
                            i + 1, lines.size());

                    if (!batchErrors.isEmpty()) {
                        errorRecordRepository.bulkInsertRecordsWithCauses(batchErrors, ingestion.getId());
                        totalValidationErrors += batchErrors.size();
                    }

                    if (!batchDaticontabili.isEmpty()) {
                        stagingRepository.bulkLoadDatiContabiliToStaging(batchDaticontabili, ingestion.getId(), submission.getId());
                        totalParsed += batchDaticontabili.size();
                    }

                    batchDaticontabili.clear();
                    batchErrors.clear();
                }
            }
        }

        lines.clear();

        log.info("Loaded {} daticontabili to staging in {} batches, {} validation errors, in {}ms",
                totalParsed, batchNumber, totalValidationErrors, System.currentTimeMillis() - startTime);

        long processStart = System.currentTimeMillis();
        StagingResult stagingResult = stagingRepository.processDatiContabiliFromStaging(submission.getId());
        log.info("Set-based processing completed in {}ms", System.currentTimeMillis() - processStart);

        if (stagingResult.duplicateCount() > 0) {
            createErrorRecordsForDuplicateDatiContabili(ingestion, submission);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Completed daticontabili ingestion in {}ms: parsed={}, inserted={}, duplicates={}, validationErrors={}",
                elapsed, totalParsed, stagingResult.insertedCount(), stagingResult.duplicateCount(), totalValidationErrors);

        return new StagingResult(stagingResult.insertedCount(), stagingResult.duplicateCount(), 0, totalValidationErrors);
    }

    @Override
    public StagingResult processCollegamentiFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException {
        log.info("Starting STREAMING staging-based collegamenti ingestion for file: {}, submission: {}",
                file.name(), submission.getId());
        long startTime = System.currentTimeMillis();

        List<String> lines = readAllLines(file);
        if (lines.isEmpty()) {
            return new StagingResult(0, 0);
        }

        int totalLines = lines.size();
        log.info("Read {} lines from file in {}ms", totalLines, System.currentTimeMillis() - startTime);

        // Validate header/footer
//        validator.validateHeader(lines.getFirst(), ingestion.getIngestionType().getName());
//        validator.validateFooter(lines.getLast(), lines.size(), ingestion.getIngestionType().getName());
//        lines.removeFirst();
//        lines.removeLast();

        // Phase 2: Process in BATCHES to avoid OOM
        int totalValidationErrors = 0;
        int totalParsed = 0;
        int batchNumber = 0;

        List<it.deloitte.postrxade.entity.Collegamenti> batchCollegamenti = new ArrayList<>();
        List<ErrorRecord> batchErrors = new ArrayList<>();
        Set<String> fileLevelCollegamenti = new HashSet<>();

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            try {
                ErrorRecord error = processCollegamentiLine(line, ingestion, submission, batchCollegamenti, fileLevelCollegamenti);
                if (error != null) {
                    batchErrors.add(error);
                }
            } catch (Exception e) {
                if (totalValidationErrors < 10) {
                    log.warn("Error processing collegamenti line {}: {}", i + 1, e.getMessage());
                }
                try {
                    ErrorRecord error = createErrorRecordFromException(e, line, ingestion, submission);
                    batchErrors.add(error);
                } catch (NotFoundRecordException ex) {
                    log.error("Failed to create error record for exception", ex);
                }
            }

            if (batchCollegamenti.size() >= MERCHANT_PARSE_BATCH_SIZE || i == lines.size() - 1) {
                if (!batchCollegamenti.isEmpty() || !batchErrors.isEmpty()) {
                    batchNumber++;
                    log.info("Processing batch {}: {} collegamenti, {} errors (total parsed: {}/{})",
                            batchNumber, batchCollegamenti.size(), batchErrors.size(),
                            i + 1, lines.size());

                    if (!batchErrors.isEmpty()) {
                        errorRecordRepository.bulkInsertRecordsWithCauses(batchErrors, ingestion.getId());
                        totalValidationErrors += batchErrors.size();
                    }

                    if (!batchCollegamenti.isEmpty()) {
                        stagingRepository.bulkLoadCollegamentiToStaging(batchCollegamenti, ingestion.getId(), submission.getId());
                        totalParsed += batchCollegamenti.size();
                    }

                    batchCollegamenti.clear();
                    batchErrors.clear();
                }
            }
        }

        lines.clear();

        log.info("Loaded {} collegamenti to staging in {} batches, {} validation errors, in {}ms",
                totalParsed, batchNumber, totalValidationErrors, System.currentTimeMillis() - startTime);

        long processStart = System.currentTimeMillis();
        StagingResult stagingResult = stagingRepository.processCollegamentiFromStaging(submission.getId());
        log.info("Set-based processing completed in {}ms", System.currentTimeMillis() - processStart);

        if (stagingResult.duplicateCount() > 0) {
            createErrorRecordsForDuplicateCollegamenti(ingestion, submission);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Completed collegamenti ingestion in {}ms: parsed={}, inserted={}, duplicates={}, validationErrors={}",
                elapsed, totalParsed, stagingResult.insertedCount(), stagingResult.duplicateCount(), totalValidationErrors);

        return new StagingResult(stagingResult.insertedCount(), stagingResult.duplicateCount(), 0, totalValidationErrors);
    }

    @Override
    public StagingResult processCambioNdgFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException {
        log.info("Starting STREAMING staging-based cambiondg ingestion for file: {}, submission: {}",
                file.name(), submission.getId());
        long startTime = System.currentTimeMillis();

        List<String> lines = readAllLines(file);
        if (lines.isEmpty()) {
            return new StagingResult(0, 0);
        }

        int totalLines = lines.size();
        log.info("Read {} lines from file in {}ms", totalLines, System.currentTimeMillis() - startTime);

        // Validate header/footer
//        validator.validateHeader(lines.getFirst(), ingestion.getIngestionType().getName());
//        validator.validateFooter(lines.getLast(), lines.size(), ingestion.getIngestionType().getName());
//        lines.removeFirst();
//        lines.removeLast();

        // Phase 2: Process in BATCHES to avoid OOM
        int totalValidationErrors = 0;
        int totalParsed = 0;
        int batchNumber = 0;

        List<it.deloitte.postrxade.entity.CambioNdg> batchCambioNdg = new ArrayList<>();
        List<ErrorRecord> batchErrors = new ArrayList<>();
        Set<String> fileLevelCambioNdg = new HashSet<>();

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            try {
                ErrorRecord error = processCambioNdgLine(line, ingestion, submission, batchCambioNdg, fileLevelCambioNdg);
                if (error != null) {
                    batchErrors.add(error);
                }
            } catch (Exception e) {
                if (totalValidationErrors < 10) {
                    log.warn("Error processing cambiondg line {}: {}", i + 1, e.getMessage());
                }
                try {
                    ErrorRecord error = createErrorRecordFromException(e, line, ingestion, submission);
                    batchErrors.add(error);
                } catch (NotFoundRecordException ex) {
                    log.error("Failed to create error record for exception", ex);
                }
            }

            if (batchCambioNdg.size() >= MERCHANT_PARSE_BATCH_SIZE || i == lines.size() - 1) {
                if (!batchCambioNdg.isEmpty() || !batchErrors.isEmpty()) {
                    batchNumber++;
                    log.info("Processing batch {}: {} cambiondg, {} errors (total parsed: {}/{})",
                            batchNumber, batchCambioNdg.size(), batchErrors.size(),
                            i + 1, lines.size());

                    if (!batchErrors.isEmpty()) {
                        errorRecordRepository.bulkInsertRecordsWithCauses(batchErrors, ingestion.getId());
                        totalValidationErrors += batchErrors.size();
                    }

                    if (!batchCambioNdg.isEmpty()) {
                        stagingRepository.bulkLoadCambioNdgToStaging(batchCambioNdg, ingestion.getId(), submission.getId());
                        totalParsed += batchCambioNdg.size();
                    }

                    batchCambioNdg.clear();
                    batchErrors.clear();
                }
            }
        }

        lines.clear();

        log.info("Loaded {} cambiondg to staging in {} batches, {} validation errors, in {}ms",
                totalParsed, batchNumber, totalValidationErrors, System.currentTimeMillis() - startTime);

        long processStart = System.currentTimeMillis();
        StagingResult stagingResult = stagingRepository.processCambioNdgFromStaging(submission.getId());
        log.info("Set-based processing completed in {}ms", System.currentTimeMillis() - processStart);

        if (stagingResult.duplicateCount() > 0) {
            createErrorRecordsForDuplicateCambioNdg(ingestion, submission);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Completed cambiondg ingestion in {}ms: parsed={}, inserted={}, duplicates={}, validationErrors={}",
                elapsed, totalParsed, stagingResult.insertedCount(), stagingResult.duplicateCount(), totalValidationErrors);

        return new StagingResult(stagingResult.insertedCount(), stagingResult.duplicateCount(), 0, totalValidationErrors);
    }

    /**
     * Create an ErrorRecord with causes.
     */
    private ErrorRecord createErrorRecord(
            List<ErrorRecordCause> errorCauses,
            String line,
            Ingestion ingestion,
            Submission submission) throws NotFoundRecordException {

        ErrorRecord errorRecord = new ErrorRecord();
        errorRecord.setRawRow(line);
        errorRecord.setIngestion(ingestion);
        errorRecord.setSubmission(submission);

        List<ErrorCause> causes = new ArrayList<>();
        for (ErrorRecordCause cause : errorCauses) {
            ErrorType errorType = errorTypeService.getErrorType(cause.errorCode());

            ErrorCause errorCause = new ErrorCause();
            errorCause.setErrorRecord(errorRecord);
            errorCause.setErrorMessage(cause.description());
            errorCause.setErrorType(errorType);
            errorCause.setSubmission(submission);
            causes.add(errorCause);
        }
        errorRecord.setErrorCauses(causes);
        return errorRecord;
    }

    /**
     * Create an ErrorRecord from an exception.
     */
    private ErrorRecord createErrorRecordFromException(
            Exception exception,
            String line,
            Ingestion ingestion,
            Submission submission) throws NotFoundRecordException {

        ErrorRecord errorRecord = new ErrorRecord();
        errorRecord.setRawRow(line);
        errorRecord.setIngestion(ingestion);
        errorRecord.setSubmission(submission);

        ErrorType errorType = errorTypeService.getErrorType(ErrorTypeCode.INVALID_FORMAT.getErrorCode());

        ErrorCause errorCause = new ErrorCause();
        errorCause.setErrorRecord(errorRecord);
        errorCause.setErrorMessage("Parse error: " + exception.getMessage());
        errorCause.setErrorType(errorType);
        errorCause.setSubmission(submission);

        errorRecord.setErrorCauses(List.of(errorCause));
        return errorRecord;
    }

    // ==================== UTILITY METHODS ====================

    /**
     * Read all lines from file.
     */
    private List<String> readAllLines(RemoteFile file) throws IOException {
        List<String> lines = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(file.stream(), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    lines.add(line);
                }
            }
        }

        return lines;
    }
}
