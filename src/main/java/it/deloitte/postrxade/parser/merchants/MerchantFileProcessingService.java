package it.deloitte.postrxade.parser.merchants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.enums.ErrorTypeCode;
import it.deloitte.postrxade.enums.IngestionTypeEnum;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.parser.merchants.types.DatiContabiliRecord;
import it.deloitte.postrxade.parser.merchants.types.SoggettiRecord;
import it.deloitte.postrxade.parser.transaction.RemoteFile;
import it.deloitte.postrxade.records.ErrorRecordCause;
import it.deloitte.postrxade.records.ProcessedRecordBatch;
import it.deloitte.postrxade.repository.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import it.deloitte.postrxade.service.ErrorTypeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ma.glasnost.orika.MapperFacade;

@RequiredArgsConstructor
@Service
@Slf4j
public class MerchantFileProcessingService {

    private final MerchantFileLineParser parser = new MerchantFileLineParser();
    private final MerchantFileLineValidator validator = new MerchantFileLineValidator();

    @Qualifier("mapperFacade")
    private final MapperFacade mapperFacade;
    private final TransactionRepository transactionRepository;
    private final ResolvedTransactionRepository resolvedTransactionRepository;
    private final MerchantRepository merchantRepository;
    private final SoggettiRepository soggettiRepository;
    private final RapportiRepository rapportiRepository;
    private final DatiContabiliRepository datiContabiliRepository;
    private final CollegamentiRepository collegamentiRepository;
    private final CambioNdgRepository cambioNdgRepository;
    private final ErrorTypeService errorTypeService;

    // Batch size optimized based on testing: 1000 provides best balance
    // Test results: 500->666s total, 1000->591s total (best), 10000->774s total
    // Larger batches increase saving time due to larger transactions
    private static final int BATCH_SIZE = 1000;
    // Thread pool size: Original configuration (was causing 97% CPU usage)
    // TODO: Monitor CPU after optimizations. If still high, consider reducing to cores + 1
    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    public List<ProcessedRecordBatch> process(
            RemoteFile file,
            Ingestion ingestion,
            Submission submission,
            Obligation obligation) throws IOException {
        log.info("Starting parallel processing of file: {}, ingestionId: {}, ingestionType: {}, threads: {}",
                file.name(), ingestion.getId(), ingestion.getIngestionType().getName(), THREAD_POOL_SIZE);

        List<String> allLines = readAllLines(file);
        log.info("Read {} lines from file: {}", allLines.size(), file.name());

        if (allLines.isEmpty()) {
            log.info("No records to process in file: {}", file.name());
            return new ArrayList<>();
        }

        log.debug("About to process {} records for ingestionId: {}", allLines.size(), ingestion.getId());

        List<ProcessedRecordBatch> processedBatches = processRecordsInParallel(allLines, ingestion, submission, obligation);

        log.info("Completed parallel processing of file: {}. Returning {} batches for saving",
                file.name(), processedBatches.size());
        log.debug("Exiting process() for file: {}, ingestionId: {}", file.name(), ingestion.getId());

        return processedBatches;
    }

    private List<String> readAllLines(RemoteFile file) throws IOException {
        log.debug("Entering readAllLines() for file: {}", file != null ? file.name() : null);
        List<String> lines = new ArrayList<>();
        int blankLinesSkipped = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(file.stream(), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    blankLinesSkipped++;
                    continue;
                }
                lines.add(line);
            }
        }

        log.debug("Read {} valid lines, skipped {} blank lines", lines.size(), blankLinesSkipped);
        log.debug("Exiting readAllLines() for file: {} with {} lines", file != null ? file.name() : null, lines.size());
        return lines;
    }

    private List<ProcessedRecordBatch> processRecordsInParallel(
            List<String> lines,
            Ingestion ingestion,
            Submission submission,
            Obligation obligation) {
        log.debug("Entering processRecordsInParallel() with {} lines for ingestionId: {}", lines != null ? lines.size() : 0,
                ingestion != null ? ingestion.getId() : null);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        AtomicInteger soggettiCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        try {
            List<List<String>> batches = partitionList(lines);
            log.info("Processing {} records in {} batches using {} threads",
                    lines.size(), batches.size(), THREAD_POOL_SIZE);

            List<CompletableFuture<ProcessedRecordBatch>> futures = new ArrayList<>();
            log.debug("Created {} batches with batch size: {}", batches.size(), BATCH_SIZE);

            Set<String> fileLevelMerchants = ConcurrentHashMap.newKeySet();

            for (int i = 0; i < batches.size(); i++) {
                final int batchIndex = i;
                final List<String> batch = batches.get(i);
                log.debug("Submitting batch {} to executor with {} records", batchIndex, batch.size());

                CompletableFuture<ProcessedRecordBatch> future = CompletableFuture.supplyAsync(() -> {
                    log.debug("Async execution started for batch {}", batchIndex);
                    try {
                        ProcessedRecordBatch result = processBatch(batch, ingestion, submission, obligation, batchIndex, fileLevelMerchants);
                        log.debug("Async execution completed for batch {}", batchIndex);
                        return result;
                    } catch (NotFoundRecordException e) {
                        log.error("Error processing batch {}: {}", batchIndex, e.getMessage());
                        throw new CompletionException(e);
                    }
                }, executor);

                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            List<ProcessedRecordBatch> processedBatches = new ArrayList<>();
            for (CompletableFuture<ProcessedRecordBatch> future : futures) {
                ProcessedRecordBatch result = future.join();
                processedBatches.add(result);
                soggettiCount.addAndGet(result.soggettiList().size());
                errorCount.addAndGet(result.errorRecords().size());
            }

            // Clear futures list to free memory (results are already in processedBatches)
            futures.clear();
            // Note: fileLevelMerchants is kept for potential future use, but could be cleared if not needed

            return processedBatches;

        } finally {
            shutdownExecutor(executor);
        }
    }

    /**
     * Process records with streaming callback to save batches incrementally.
     * This method processes batches and immediately invokes the callback for each batch,
     * allowing the caller to save to DB in the main thread without accumulating all batches in memory.
     *
     * @param file          The file to process
     * @param ingestion     The ingestion entity
     * @param submission    The submission entity
     * @param obligation    The obligation entity
     * @param savingService The service that will handle batch saving
     * @throws IOException if file reading fails
     */
    public void processWithImmediateSave(
            RemoteFile file,
            Ingestion ingestion,
            Submission submission,
            Obligation obligation,
            Object savingService) throws IOException {
        log.info("Starting parallel processing with immediate save for file: {}, ingestionId: {}, ingestionType: {}, threads: {}",
                file.name(), ingestion.getId(), ingestion.getIngestionType().getName(), THREAD_POOL_SIZE);

        List<String> allLines = readAllLines(file);
        log.info("Read {} lines from file: {}", allLines.size(), file.name());

        if (allLines.isEmpty()) {
            log.info("No records to process in file: {}", file.name());
            return;
        }

        log.debug("About to process {} records for ingestionId: {}", allLines.size(), ingestion.getId());

        processRecordsInParallelWithCallback(allLines, ingestion, submission, obligation, savingService);

        log.info("Completed parallel processing with immediate save for file: {}",
                file.name());
    }

    private void processRecordsInParallelWithCallback(
            List<String> lines,
            Ingestion ingestion,
            Submission submission,
            Obligation obligation,
            Object savingService) {
        log.debug("Entering processRecordsInParallelWithCallback() with {} lines for ingestionId: {}", lines.size(), ingestion.getId());
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        BlockingQueue<ProcessedRecordBatch> batchQueue = new LinkedBlockingQueue<>();
        AtomicInteger completedBatches = new AtomicInteger(0);
        List<List<String>> batches = partitionList(lines);
        int totalBatches = batches.size();

        try {
            log.info("Processing {} records in {} batches using {} threads",
                    lines.size(), totalBatches, THREAD_POOL_SIZE);

            Set<String> fileLevelSoggatti = ConcurrentHashMap.newKeySet();
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int i = 0; i < batches.size(); i++) {
                final int batchIndex = i;
                final List<String> batch = batches.get(i);
                log.debug("Submitting batch {} to executor with {} records", batchIndex, batch.size());

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        log.debug("Async execution started for batch {}", batchIndex);
                        ProcessedRecordBatch result = processBatch(batch, ingestion, submission, obligation, batchIndex, fileLevelSoggatti);
                        log.debug("Async execution completed for batch {}, queuing for save", batchIndex);
                        batchQueue.put(result); // Put the result in the queue for immediate saving
                    } catch (NotFoundRecordException | InterruptedException e) {
                        log.error("Error processing batch {}: {}", batchIndex, e.getMessage());
                        throw new CompletionException(e);
                    }
                }, executor);

                futures.add(future);
            }

            // Main thread consumes batches from queue and saves them immediately
            Thread savingThread = new Thread(() -> {
                try {
                    // Cast to ObligationServiceImpl to call the saving method
                    it.deloitte.postrxade.service.impl.ObligationServiceImpl obligationService =
                            (it.deloitte.postrxade.service.impl.ObligationServiceImpl) savingService;

                    while (completedBatches.get() < totalBatches) {
                        ProcessedRecordBatch batch = batchQueue.poll(10, TimeUnit.SECONDS);
                        if (batch != null) {
                            log.debug("Main thread saving batch, completed: {}/{}", completedBatches.get() + 1, totalBatches);
                            obligationService.saveProcessedBatches(batch, ingestion, submission);
                            completedBatches.incrementAndGet();
                            log.debug("Batch saved successfully, memory cleared for next batch");
                        }
                    }
                } catch (InterruptedException e) {
                    log.error("Saving thread interrupted", e);
                    Thread.currentThread().interrupt();
                }
            });

            savingThread.start();

            // Wait for all processing to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // Wait for saving thread to finish
            savingThread.join();

            log.info("Parallel processing with immediate save complete for ingestionId: {}. All {} batches processed and saved",
                    ingestion.getId(), totalBatches);

        } catch (InterruptedException e) {
            log.error("Processing interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            shutdownExecutor(executor);
        }
    }

    private void shutdownExecutor(ExecutorService executor) {
        log.debug("Shutting down executor service in processRecordsInParallel()");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Executor termination interrupted, forcing shutdownNow()", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.debug("Executor service shutdown complete for processRecordsInParallel()");
    }

    private ProcessedRecordBatch processBatch(
            List<String> batch,
            Ingestion ingestion,
            Submission submission,
            Obligation obligation,
            int batchIndex,
            Set<String> fileLevelSoggatti) throws NotFoundRecordException {
        log.debug("Processing batch {} with {} records", batchIndex, batch.size());

        List<Soggetti> soggettiList = new ArrayList<>();
        List<Rapporti> rapportiList = new ArrayList<>();
        List<Collegamenti> collegamentiList = new ArrayList<>();
        List<CambioNdg> cambioNdgList = new ArrayList<>();
        List<DatiContabili> datiContabiliList = new ArrayList<>();
        List<ErrorRecord> errorRecords = new ArrayList<>();

        String ingestionTypeName = ingestion.getIngestionType().getName();

        processBatchLines(batch, ingestion, submission, obligation, batchIndex,
                ingestionTypeName, soggettiList, rapportiList, collegamentiList,
                cambioNdgList, datiContabiliList, errorRecords, fileLevelSoggatti);

        return new ProcessedRecordBatch(soggettiList, rapportiList, collegamentiList,
                cambioNdgList, datiContabiliList, errorRecords);
    }

    private void processBatchLines(
            List<String> batch,
            Ingestion ingestion,
            Submission submission,
            Obligation obligation,
            int batchIndex,
            String ingestionTypeName,
            List<Soggetti> soggettiList,
            List<Rapporti> rapportiList,
            List<Collegamenti> collegamentiList,
            List<CambioNdg> cambioNdgList,
            List<DatiContabili> datiContabiliList,
            List<ErrorRecord> errorRecords,
            Set<String> fileLevelSoggatti) throws NotFoundRecordException {
        for (String line : batch) {
            log.trace("Processing line in batch {}: {}", batchIndex, line);
            try {
                ErrorRecord error = processLine(line, ingestion, submission, obligation,
                        ingestionTypeName, soggettiList, rapportiList, collegamentiList,
                        cambioNdgList, datiContabiliList, fileLevelSoggatti);
                if (error != null) {
                    errorRecords.add(error);
                    log.trace("Added error record for line in batch {}. Current error count: {}", batchIndex, errorRecords.size());
                }
            } catch (Exception e) {
                log.error("Unexpected error processing line: {}", line, e);
                ErrorRecord error = createErrorRecordFromException(e, line, ingestion, submission);
                errorRecords.add(error);
            }
        }
        validateExisting(ingestion, submission, soggettiList, rapportiList, collegamentiList, cambioNdgList, datiContabiliList, errorRecords);
    }

    private void validateExisting(
            Ingestion ingestion,
            Submission submission,
            List<Soggetti> soggettiList,
            List<Rapporti> rapportiList,
            List<Collegamenti> collegamentiList,
            List<CambioNdg> cambioNdgList,
            List<DatiContabili> datiContabiliList,
            List<ErrorRecord> errorRecords) throws NotFoundRecordException {
        if (ingestion.getIngestionType().getName().equals(IngestionTypeEnum.SOGGETTI.name())) {
            if (!soggettiList.isEmpty()) validateSoggetti(soggettiList, errorRecords, ingestion, submission);
        } else if (ingestion.getIngestionType().getName().equals(IngestionTypeEnum.RAPPORTI.name())) {
            if (!rapportiList.isEmpty()) validateRapporti(rapportiList, errorRecords, ingestion, submission);
        } else if (ingestion.getIngestionType().getName().equals(IngestionTypeEnum.DATI_CONTABILI.name())) {
            if (!datiContabiliList.isEmpty()) validateDatiContabili(datiContabiliList, errorRecords, ingestion, submission);
        } else if (ingestion.getIngestionType().getName().equals(IngestionTypeEnum.COLLEGAMENTI.name())) {
            if (!collegamentiList.isEmpty()) validateCollegamenti(collegamentiList, errorRecords, ingestion, submission);
        } else if (ingestion.getIngestionType().getName().equals(IngestionTypeEnum.CAMBIO_NDG.name())) {
            if (!cambioNdgList.isEmpty()) validateCambioNdg(cambioNdgList, errorRecords, ingestion, submission);
        }
    }

    private void validateSoggetti(
            List<Soggetti> soggettiList,
            List<ErrorRecord> errorRecords,
            Ingestion ingestion,
            Submission submission) throws NotFoundRecordException {
        List<Soggetti> updatedSoggetti = new ArrayList<>();
        Map<String, Integer> existingMap = soggettiRepository.checkExisting(soggettiList, submission);
        
        // CRITICAL: Check for missing Collegamenti parent (FK validation)
        Map<String, Integer> collegamentiMap = collegamentiRepository.checkCollegamentiForSoggetti(soggettiList, submission);

        for (Soggetti soggetti : soggettiList) {
            String key = soggetti.getIntermediario();
            Integer existsFlag = existingMap.getOrDefault(key, 0);
            
            // Check for missing Collegamenti parent
            String collegamentiKey = soggetti.getNdg() + "_" + submission.getId();
            Integer hasCollegamenti = collegamentiMap.getOrDefault(collegamentiKey, 0);
            
            if (hasCollegamenti == 0) {
                // Missing Collegamenti parent - create error record
                String description = "Soggetti cannot be created: missing Collegamenti parent for ndg: " + soggetti.getNdg();
                log.warn("Missing Collegamenti parent for Soggetti - ndg={}, submission={}", 
                        soggetti.getNdg(), submission.getId());

                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
                        description,
                        ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode()
                );

                ErrorRecord error = createErrorRecord(List.of(errorWithDescription), soggetti.getRawRow(), ingestion, submission);
                errorRecords.add(error);
            } else if (existsFlag == 1) {
                String description = "Soggetti is not created as it has a duplicate in db";

                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
                        description,
                        ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode()
                );

                ErrorRecord error = createErrorRecord(List.of(errorWithDescription), soggetti.getRawRow(), ingestion, submission);
                errorRecords.add(error);
            } else {
                updatedSoggetti.add(soggetti);
            }
        }

        soggettiList.clear();
        soggettiList.addAll(updatedSoggetti);
    }

    private void validateRapporti(
            List<Rapporti> rapportiList,
            List<ErrorRecord> errorRecords,
            Ingestion ingestion,
            Submission submission) throws NotFoundRecordException {
        List<Rapporti> updatedRapporti = new ArrayList<>();
        Map<String, Integer> existingMap = rapportiRepository.checkExisting(rapportiList, submission);
        
        // CRITICAL: Check for missing Collegamenti parent (FK validation)
        Map<String, Integer> collegamentiMap = collegamentiRepository.checkCollegamentiForRapporti(rapportiList, submission);

        for (Rapporti rapporti : rapportiList) {
            String key = rapporti.getIntermediario() + "_" + rapporti.getChiaveRapporto() + "_" + submission.getId();
            Integer existsFlag = existingMap.getOrDefault(key, 0);
            
            // Check for missing Collegamenti parent
            String collegamentiKey = rapporti.getChiaveRapporto() + "_" + submission.getId();
            Integer hasCollegamenti = collegamentiMap.getOrDefault(collegamentiKey, 0);
            
            if (hasCollegamenti == 0) {
                // Missing Collegamenti parent - create error record
                String description = "Rapporti cannot be created: missing Collegamenti parent for chiave_rapporto: " + rapporti.getChiaveRapporto();
                log.warn("Missing Collegamenti parent for Rapporti - chiave_rapporto={}, submission={}", 
                        rapporti.getChiaveRapporto(), submission.getId());

                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
                        description,
                        ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode()
                );
                errorRecords.add(createErrorRecord(List.of(errorWithDescription), rapporti.getRawRow(), ingestion, submission));
            } else if (existsFlag == 1) {
                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
                        "Rapporti is not created as it has a duplicate in db",
                        ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode()
                );
                errorRecords.add(createErrorRecord(List.of(errorWithDescription), rapporti.getRawRow(), ingestion, submission));
            } else {
                updatedRapporti.add(rapporti);
            }
        }
        rapportiList.clear();
        rapportiList.addAll(updatedRapporti);
    }

    private void validateDatiContabili(
            List<DatiContabili> datiContabiliList,
            List<ErrorRecord> errorRecords,
            Ingestion ingestion,
            Submission submission) throws NotFoundRecordException {
        List<DatiContabili> updatedDatiContabili = new ArrayList<>();
        Map<String, Integer> existingMap = datiContabiliRepository.checkExisting(datiContabiliList, submission);
        
        // CRITICAL: Check for missing Collegamenti parent (FK validation)
        Map<String, Integer> collegamentiMap = collegamentiRepository.checkCollegamentiForDatiContabili(datiContabiliList, submission);

        for (DatiContabili datiContabili : datiContabiliList) {
            String key = datiContabili.getIntermediario();
            Integer existsFlag = existingMap.getOrDefault(key, 0);
            
            // Check for missing Collegamenti parent
            String collegamentiKey = datiContabili.getChiaveRapporto() + "_" + submission.getId();
            Integer hasCollegamenti = collegamentiMap.getOrDefault(collegamentiKey, 0);
            
            if (hasCollegamenti == 0) {
                // Missing Collegamenti parent - create error record
                String description = "Dati Contabili cannot be created: missing Collegamenti parent for chiave_rapporto: " + datiContabili.getChiaveRapporto();
                log.warn("Missing Collegamenti parent for DatiContabili - chiave_rapporto={}, submission={}", 
                        datiContabili.getChiaveRapporto(), submission.getId());

                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
                        description,
                        ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode()
                );
                errorRecords.add(createErrorRecord(List.of(errorWithDescription), datiContabili.getRawRow(), ingestion, submission));
            } else if (existsFlag == 1) {
                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
                        "Dati Contabili is not created as it has a duplicate in db",
                        ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode()
                );
                errorRecords.add(createErrorRecord(List.of(errorWithDescription), datiContabili.getRawRow(), ingestion, submission));
            } else {
                updatedDatiContabili.add(datiContabili);
            }
        }
        datiContabiliList.clear();
        datiContabiliList.addAll(updatedDatiContabili);
    }

    private void validateCollegamenti(
            List<Collegamenti> collegamentiList,
            List<ErrorRecord> errorRecords,
            Ingestion ingestion,
            Submission submission) throws NotFoundRecordException {
        List<Collegamenti> updatedCollegamenti = new ArrayList<>();
        Map<String, Integer> existingMap = collegamentiRepository.checkExisting(collegamentiList, submission);

        for (Collegamenti collegamenti : collegamentiList) {
            String key = collegamenti.getIntermediario() + "_" + collegamenti.getChiaveRapporto() + "_" + collegamenti.getNdg() + "_" + submission.getId();
            Integer existsFlag = existingMap.getOrDefault(key, 0);
            if (existsFlag == 1) {
                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
                        "Collegamenti is not created as it has a duplicate in db",
                        ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode()
                );
                errorRecords.add(createErrorRecord(List.of(errorWithDescription), collegamenti.getRawRow(), ingestion, submission));
            } else {
                updatedCollegamenti.add(collegamenti);
            }
        }
        collegamentiList.clear();
        collegamentiList.addAll(updatedCollegamenti);
    }

    private void validateCambioNdg(
            List<CambioNdg> cambioNdgList,
            List<ErrorRecord> errorRecords,
            Ingestion ingestion,
            Submission submission) throws NotFoundRecordException {
        List<CambioNdg> updatedCambioNdg = new ArrayList<>();
        Map<String, Integer> existingMap = cambioNdgRepository.checkExisting(cambioNdgList, submission);

        for (CambioNdg cambioNdg : cambioNdgList) {
            String key = cambioNdg.getIntermediario();
            Integer existsFlag = existingMap.getOrDefault(key, 0);
            if (existsFlag == 1) {
                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
                        "Cambio NDG is not created as it has a duplicate in db",
                        ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode()
                );
                errorRecords.add(createErrorRecord(List.of(errorWithDescription), cambioNdg.getRawRow(), ingestion, submission));
            } else {
                updatedCambioNdg.add(cambioNdg);
            }
        }
        cambioNdgList.clear();
        cambioNdgList.addAll(updatedCambioNdg);
    }

//    private void validateMerchants(
//            List<Merchant> merchants,
//            List<ErrorRecord> errorRecords,
//            Ingestion ingestion,
//            Submission submission) throws NotFoundRecordException {
//        List<Merchant> updatedMerchants = new ArrayList<>();
//        Map<String, Integer> existingMap = merchantRepository.checkExisting(merchants, submission);
//
//        for (Merchant merchant : merchants) {
//            String key = merchant.getIdIntermediario() + "_" + merchant.getIdEsercente() + "_" + submission.getId();
//            Integer existsFlag = existingMap.getOrDefault(key, 0);
//            if (existsFlag == 1) {
//                String description = "Merchant is not created as it has a duplicate in db";
//                log.warn("Duplicate merchant found for idIntermediario={}, idEsercente={}, submission_fk={}",
//                        merchant.getIdIntermediario(), merchant.getIdEsercente(), submission.getId());
//
//                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
//                        description,
//                        ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode()
//                );
//
//                ErrorRecord error = createErrorRecord(List.of(errorWithDescription), merchant.getRawRow(), ingestion, submission);
//                errorRecords.add(error);
//            } else {
//                updatedMerchants.add(merchant);
//            }
//        }
//
//        merchants.clear();
//        merchants.addAll(updatedMerchants);
//    }

//    private void validateTransactions(
//            List<Transaction> transactions,
//            List<ErrorRecord> errorRecords,
//            Ingestion ingestion,
//            Submission submission) throws NotFoundRecordException {
//        List<Transaction> updatedTransactions = new ArrayList<>();
//
//        Map<String, Integer> existingTransactionMap =
//                transactionRepository.checkExisting(transactions);
//        Map<String, Integer> existingMerchantMap =
//                merchantRepository.checkExistingByTransactions(transactions);
//
//        for (Transaction transaction : transactions) {
//            String trasnsactionKey = transaction.getIdEsercente()
//                    + "_" + transaction.getChiaveBanca()
//                    + "_" + transaction.getIdPos()
//                    + "_" + transaction.getTipoOpe()
//                    + "_" + transaction.getDtOpe()
//                    + "_" + transaction.getDivisaOpe();
//
//            String merchantKey = transaction.getIdEsercente()
//                    + "_" + transaction.getIdIntermediario();
//
//            Integer transactionExistsFlag = existingTransactionMap.getOrDefault(trasnsactionKey, 0);
//            Integer merchantExistsFlag = existingMerchantMap.getOrDefault(merchantKey, 0);
//
//            if (merchantExistsFlag == null || merchantExistsFlag == 0) {
//                String description = "Transaction is not created as no merchant with same intermediario: " + transaction.getIdIntermediario() + " and esercente: " + transaction.getIdEsercente();
//                log.warn("No merchant found for transaction - idIntermediario={}, idEsercente={}",
//                        transaction.getIdIntermediario(), transaction.getIdEsercente());
//                ErrorRecordCause errorWithDescription = new ErrorRecordCause(
//                        description,
//                        ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode()
//                );
//
//                ErrorRecord error = createErrorRecord(List.of(errorWithDescription), transaction.getRawRow(), ingestion, submission);
//                errorRecords.add(error);
//            } else {
//                if (transactionExistsFlag == 1) {
//                    String description = "Transaction is not created as it has a duplicate in db";
//                    log.warn("Duplicate transaction found - esercente={}, chiaveBanca={}, idPos={}, tipoOpe={}, dtOpe={}, divisaOpe={}",
//                            transaction.getIdEsercente(), transaction.getChiaveBanca(), transaction.getIdPos(),
//                            transaction.getTipoOpe(), transaction.getDtOpe(), transaction.getDivisaOpe());
//
//                    ErrorRecordCause errorWithDescription = new ErrorRecordCause(
//                            description,
//                            ErrorTypeCode.FOREIGN_KEY_ERROR.getErrorCode()
//                    );
//
//                    ErrorRecord error = createErrorRecord(List.of(errorWithDescription), transaction.getRawRow(), ingestion, submission);
//                    errorRecords.add(error);
//                } else {
//                    updatedTransactions.add(transaction);
//                }
//            }
//        }
//
//        transactions.clear();
//        transactions.addAll(updatedTransactions);
//    }


    private ErrorRecord processLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            Obligation obligation,
            String ingestionTypeName,
            List<Soggetti> soggettiList,
            List<Rapporti> rapportiList,
            List<Collegamenti> collegamentiList,
            List<CambioNdg> cambioNdgList,
            List<DatiContabili> datiContabiliList,
            Set<String> fileLevelSoggatti) throws NotFoundRecordException {
        if (ingestionTypeName.equals(IngestionTypeEnum.SOGGETTI.name())) {
            return processSoggettiLine(line, ingestion, submission, soggettiList, fileLevelSoggatti);
        } else if (ingestionTypeName.equals(IngestionTypeEnum.RAPPORTI.name())) {
            return processRapportiLine(line, ingestion, submission, rapportiList, fileLevelSoggatti);
        } else if (ingestionTypeName.equals(IngestionTypeEnum.DATI_CONTABILI.name())) {
            return processDatiContabiliLine(line, ingestion, submission, datiContabiliList, fileLevelSoggatti);
        } else if (ingestionTypeName.equals(IngestionTypeEnum.COLLEGAMENTI.name())) {
            return processCollegamentiLine(line, ingestion, submission, collegamentiList, fileLevelSoggatti);
        } else if (ingestionTypeName.equals(IngestionTypeEnum.CAMBIO_NDG.name())) {
            return processCambioNdgLine(line, ingestion, submission, cambioNdgList, fileLevelSoggatti);
        }
        return null;
    }

    private ErrorRecord processSoggettiLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<Soggetti> soggettiList,
            Set<String> fileLevelMerchants) throws
            NotFoundRecordException {
        SoggettiRecord record = parser.parseSoggettiLine(line);
        log.debug("Processing soggetti record {}", record);
        List<ErrorRecordCause> errorCauses = validator.validateSoggetti(record, fileLevelMerchants);
        String soggettiRecordKey = record.getIntermediario()
                + "_" + record.getNdg() + "_" + submission.getId();
        if (fileLevelMerchants.contains(soggettiRecordKey)) {
            errorCauses.add(
                    new ErrorRecordCause("Soggetti already exists in the same file", ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())
            );
        }
        if (errorCauses.isEmpty()) {
            Soggetti soggetti = mapperFacade.map(record, Soggetti.class);
            soggetti.setIngestion(ingestion);
            soggetti.setSubmission(submission);
            soggetti.setRawRow(line);
            soggettiList.add(soggetti);
            String soggettiKey = soggetti.getIntermediario()
                    + "_" + soggetti.getNdg() + "_" + submission.getId();
            fileLevelMerchants.add(soggettiKey);
        } else {
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
        return null;
    }

    private ErrorRecord processRapportiLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<Rapporti> rapportiList,
            Set<String> fileLevelMerchants) throws NotFoundRecordException {
        it.deloitte.postrxade.parser.merchants.types.RapportiRecord record = parser.parseRapportoLine(line);
        log.debug("Processing rapporti record {}", record);
        List<ErrorRecordCause> errorCauses = validator.validateRapporto(record);
        String rapportiRecordKey = record.getIntermediario() + "_" + record.getChiaveRapporto();
        if (fileLevelMerchants.contains(rapportiRecordKey)) {
            errorCauses.add(
                    new ErrorRecordCause("Rapporto already exists in the same file", ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())
            );
        }
        if (errorCauses.isEmpty()) {
            Rapporti rapporto = mapperFacade.map(record, Rapporti.class);
            rapporto.setIngestion(ingestion);
            rapporto.setSubmission(submission);
            rapporto.setRawRow(line);
            rapportiList.add(rapporto);
            fileLevelMerchants.add(rapportiRecordKey);
        } else {
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
        return null;
    }

    private ErrorRecord processDatiContabiliLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<DatiContabili> datiContabiliList,
            Set<String> fileLevelMerchants) throws NotFoundRecordException {
        DatiContabiliRecord record = parser.parseDatiContabiliLine(line);
        log.debug("Processing dati contabili record {}", record);
        List<ErrorRecordCause> errorCauses = validator.validateDatiContabili(record, fileLevelMerchants);

        String datiContabiliRecordKey = record.getIntermediario() + "_" + record.getChiaveRapporto() + "_" + submission.getId();
        if (fileLevelMerchants.contains(datiContabiliRecordKey)) {
            errorCauses.add(
                    new ErrorRecordCause("Dati Contabili already exists in the same file", ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())
            );
        }
        if (errorCauses.isEmpty()) {
            DatiContabili datiContabili = mapperFacade.map(record, DatiContabili.class);
            datiContabili.setIngestion(ingestion);
            datiContabili.setSubmission(submission);
            datiContabili.setRawRow(line);
            datiContabiliList.add(datiContabili);
            fileLevelMerchants.add(datiContabiliRecordKey);
        } else {
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
        return null;
    }

    private ErrorRecord processCollegamentiLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<Collegamenti> collegamentiList,
            Set<String> fileLevelMerchants) throws NotFoundRecordException {
        it.deloitte.postrxade.parser.merchants.types.CollegamentiRecord record = parser.parseCollegamentiLine(line);
        log.debug("Processing collegamenti record {}", record);
        List<ErrorRecordCause> errorCauses = validator.validateCollegamenti(record, fileLevelMerchants);
        String collegamentiRecordKey = record.getIntermediario() + "_" + record.getChiaveRapporto() + "_" + record.getNdg();
        if (fileLevelMerchants.contains(collegamentiRecordKey)) {
            errorCauses.add(
                    new ErrorRecordCause("Collegamento already exists in the same file", ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())
            );
        }
        if (errorCauses.isEmpty()) {
            Collegamenti collegamenti = mapperFacade.map(record, Collegamenti.class);
            collegamenti.setIngestion(ingestion);
            collegamenti.setSubmission(submission);
            collegamenti.setRawRow(line);
            collegamentiList.add(collegamenti);
            fileLevelMerchants.add(collegamentiRecordKey);
        } else {
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
        return null;
    }

    private ErrorRecord processCambioNdgLine(
            String line,
            Ingestion ingestion,
            Submission submission,
            List<CambioNdg> cambioNdgList,
            Set<String> fileLevelMerchants) throws NotFoundRecordException {
        it.deloitte.postrxade.parser.merchants.types.CambioNdgRecord record = parser.parseCambioNdgLine(line);
        log.debug("Processing cambio ndg record {}", record);
        List<ErrorRecordCause> errorCauses = validator.validateCambioNdg(record, fileLevelMerchants);
        String cambioNdgRecordKey = record.getIntermediario() + "_" + record.getNdgVecchio() + "_" + record.getNdgVecchio() + "_" + submission.getId();
        if (fileLevelMerchants.contains(cambioNdgRecordKey)) {
            errorCauses.add(
                    new ErrorRecordCause("CambioNdg already exists in the same file", ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode())
            );
        }
        if (errorCauses.isEmpty()) {
            CambioNdg cambioNdg = mapperFacade.map(record, CambioNdg.class);
            cambioNdg.setIngestion(ingestion);
            cambioNdg.setSubmission(submission);
            cambioNdg.setRawRow(line);
            cambioNdgList.add(cambioNdg);
            fileLevelMerchants.add(cambioNdgRecordKey);
        } else {
            return createErrorRecord(errorCauses, line, ingestion, submission);
        }
        return null;
    }

    private List<List<String>> partitionList(List<String> list) {
        int batchSize = BATCH_SIZE;
        log.trace("Partitioning list of size {} with batchSize {}", list != null ? list.size() : 0, batchSize);
        List<List<String>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            partitions.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        log.trace("Created {} partitions from list", partitions.size());
        return partitions;
    }

    private ErrorRecord createErrorRecord(
            List<ErrorRecordCause> errorCauses,
            String line,
            Ingestion ingestion,
            Submission submission) throws NotFoundRecordException {
        log.trace("Entering createErrorRecord() for ingestionId: {}", ingestion != null ? ingestion.getId() : null);
        ErrorRecord errorRecord = new ErrorRecord();
        errorRecord.setRawRow(line);
        errorRecord.setIngestion(ingestion);
        errorRecord.setSubmission(submission);

        List<ErrorCause> casuses = new ArrayList<>();

        for (ErrorRecordCause cause : errorCauses) {
            ErrorType errorType = errorTypeService.getErrorType(cause.errorCode());

            ErrorCause errorCause = new ErrorCause();
            errorCause.setErrorRecord(errorRecord);
            errorCause.setErrorMessage(cause.description());
            errorCause.setErrorType(errorType);
            errorCause.setSubmission(submission);
            casuses.add(errorCause);
        }
        errorRecord.setErrorCauses(casuses);
        log.debug("Record validation failed for line: {} with errors: {}", line, errorCauses);
        return errorRecord;
    }

    private ErrorRecord createErrorRecordFromException(
            Exception exception,
            String line,
            Ingestion ingestion,
            Submission submission) throws NotFoundRecordException {
        log.trace("Entering createErrorRecordFromException() for ingestionId: {}", ingestion != null ? ingestion.getId() : null);
        ErrorRecord errorRecord = new ErrorRecord();
        errorRecord.setRawRow(line);
        errorRecord.setIngestion(ingestion);
        errorRecord.setSubmission(submission);

        String errorMessage = exception.getMessage() != null ? exception.getMessage() : exception.getClass().getSimpleName();
        log.debug("Record processing failed with exception for line: {} with error: {}", line, errorMessage);
        ErrorType errorType = errorTypeService.getErrorType(ErrorTypeCode.INVALID_FORMAT.getErrorCode());

        ErrorCause errorCause = new ErrorCause();
        errorCause.setErrorType(errorType);
        errorCause.setErrorMessage(errorMessage);

        List<ErrorCause> causes = new ArrayList<>();
        causes.add(errorCause);
        errorRecord.setErrorCauses(causes);

        return errorRecord;
    }
}
