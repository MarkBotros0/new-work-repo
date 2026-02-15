package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.formatter.OutputFileFormatter;
import it.deloitte.postrxade.repository.*;
import it.deloitte.postrxade.service.OutputService;
import it.deloitte.postrxade.service.S3Service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Slf4j
@Service
public class OutputServiceImpl implements OutputService {

    @Value("${output.rows-per-file:1000000}")
    private int rowsPerOutputFile;
    private static final int ROWS_PER_OUTPUT_FILE = 100000;
    private static final String OUTPUT_FILE_EXTENSION = "txt";
    private static final int UPDATE_BATCH_SIZE = 100;
    @Value("${aws.s3.output-folder}")
    private String s3OutputFolder;

    @Autowired
    private OutputRepository outputRepository;

    @Autowired
    private SubmissionRepository submissionRepository;

    @Autowired
    private CollegamentiRepository collegamentiRepository;
    @Autowired
    private RapportiRepository rapportiRepository;
    @Autowired
    private SoggettiRepository soggettiRepository;
    @Autowired
    private DatiContabiliRepository datiContabiliRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private ResolvedTransactionRepository resolvedTransactionRepository;

    @Autowired
    private S3Service s3Service;

    @Value("${aws.s3.output-folder}")
    private String outputFolder;
    private TransactionTemplate transactionTemplate;
    private PlatformTransactionManager transactionManager;


    @Override
    public List<Output> generateOutput(Submission submission) {
        if (submission == null || submission.getId() == null) {
            return Collections.emptyList();
        }

        log.info("Starting generateOutputMerchant for submissionId={}", submission.getId());

        List<Output> outputs = new ArrayList<>();
        int pageNumber = 0;
        boolean hasNextPage = true;

        while (hasNextPage) {
            // 1) Fetch a page of Collegamenti IDs with fk_output IS NULL
            List<Long> collegamentiIds = collegamentiRepository
                    .findCollegamentiIdsBySubmissionIdAndNullOutput(
                            submission.getId(),
                            rowsPerOutputFile
                    );

            log.info("Merchant page {} for submissionId={}. Collegamenti found: {} (expected max: {})",
                    pageNumber, submission.getId(), collegamentiIds.size(), rowsPerOutputFile);

            if (collegamentiIds.isEmpty()) {
                break;
            }

            if (collegamentiIds.size() > rowsPerOutputFile) {
                log.error("BUG DETECTED: Query returned {} collegamenti, LIMIT was {}. Truncating.",
                        collegamentiIds.size(), rowsPerOutputFile);
                collegamentiIds = collegamentiIds.subList(0, rowsPerOutputFile);
            }

            if (collegamentiIds.size() < rowsPerOutputFile) {
                hasNextPage = false;
            }

            final int currentPageNumber = pageNumber;
            final Long submissionId = submission.getId();

            // 2) Create Output in short transaction
            Output outputEntity = transactionTemplate.execute(status -> {
                Submission attachedSubmission = submissionRepository.findOneById(submissionId)
                        .orElseThrow(() -> new RuntimeException("Submission not found: " + submissionId));

                String submissionPath = buildSubmissionPath(attachedSubmission);

                Output output = new Output();
                output.setSubmission(attachedSubmission);
                output.setGeneratedAt(LocalDateTime.now().toString());
                output.setExtensionType(OUTPUT_FILE_EXTENSION);

                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                String fileName = String.format(
                        "ANAGRAFICA_OUTPUT_%s_%d_part%03d.%s",
                        timestamp,
                        attachedSubmission.getId(),
                        currentPageNumber + 1,
                        OUTPUT_FILE_EXTENSION
                );

                String fullPath = normalizePath(s3OutputFolder, normalizePath(submissionPath, fileName));
                output.setFullPath(fullPath);

                return outputRepository.save(output);
            });

            // 3) Fetch only the NDG/chiave_rapporto needed for dependent updates (for this page only)
            List<Object[]> ndgChiaviRows = collegamentiRepository.findNdgAndChiaviByIds(collegamentiIds);

            List<String> ndgs = ndgChiaviRows.stream()
                    .map(r -> (String) r[0])
                    .filter(Objects::nonNull)
                    .distinct()
                    .toList();

            List<String> chiavi = ndgChiaviRows.stream()
                    .map(r -> (String) r[1])
                    .filter(Objects::nonNull)
                    .distinct()
                    .toList();

            // 4) Update fk_output on MERCHANT_COLLEGAMENTI in batches
            List<List<Long>> idBatches = partitionList(collegamentiIds, UPDATE_BATCH_SIZE);

            int batchNumber = 0;
            for (List<Long> batch : idBatches) {
                batchNumber++;
                final int currentBatch = batchNumber;
                final int totalBatches = idBatches.size();

                TransactionTemplate batchTemplate = createBatchTransactionTemplate(600);
                batchTemplate.execute(status -> {
                    log.info("Updating collegamenti batch {}/{} for outputId={}, size={}",
                            currentBatch, totalBatches, outputEntity.getId(), batch.size());
                    collegamentiRepository.updateOutputForeignKey(batch, outputEntity.getId());
                    return null;
                });
            }

            // 5) Update dependent tables (same logic as old method, but page-scoped)
            if (!ndgs.isEmpty()) {
                soggettiRepository.updateOutputForeignKeyByNdgs(
                        ndgs, submission.getId(), outputEntity);
            }

            if (!chiavi.isEmpty()) {
                rapportiRepository.updateOutputForeignKeyByChiavi(
                        chiavi, submission.getId(), outputEntity);

                datiContabiliRepository.updateOutputForeignKeyByChiavi(
                        chiavi, submission.getId(), outputEntity);
            }

            outputs.add(outputEntity);
            pageNumber++;
        }

        log.info("generateOutputMerchant completed for submissionId={}. Total outputs created: {}",
                submission.getId(), outputs.size());

        return outputs;
    }

    @Override
    @Transactional(readOnly = true)
    public void generateSubmissionOutputTxt(Long submissionId) throws NotFoundRecordException, IOException {
        log.debug("Generating TXT outputs for submissionId={}", submissionId);

        Submission submission = submissionRepository.findOneById(submissionId)
                .orElseThrow(() -> new NotFoundRecordException("Submission not found: " + submissionId));

        List<Output> outputs = generateOutputMerchant(submission);

        if (outputs.isEmpty()) {
            throw new NotFoundRecordException("No data found to generate output for submission: " + submissionId);
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String zipFileName = String.format("SUBMISSION_%d_OUTPUT_%s.zip", submissionId, timestamp);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ZipOutputStream zos = new ZipOutputStream(baos, StandardCharsets.UTF_8)) {

            for (Output output : outputs) {
                // Fetch tagged data using the unique Output ID
//                List<Collegamenti> collegamentiList = collegamentiRepository.findByOutputId(output.getId());
                List<Collegamenti> collegamentiList = collegamentiRepository.findCollegamentiBySubmissionIdAndNullOutputBulkFetched(
                        submissionId, output.getId(), rowsPerOutputFile);
                List<Rapporti> rapportiList = rapportiRepository.findByOutputId(output.getId());
//                List<Soggetti> soggettiList = soggettiRepository.findByOutputId(output.getId());
//                List<DatiContabili> datiContabiliList = datiContabiliRepository.findByOutputId(output.getId());

//                log.debug("Formatting file. Soggetti: {}, Rapporti: {}, Dati: {}",
//                        soggettiList.size(), rapportiList.size(), datiContabiliList.size());

                // Use the filename generated in generateOutputMerchant
                String entryName = output.getFullPath().substring(output.getFullPath().lastIndexOf("/") + 1);
                zos.putNextEntry(new ZipEntry(entryName));

                // 1. Header
                zos.write(OutputFileFormatter.createHeader().getBytes(StandardCharsets.UTF_8));

                // 2. Rapporti
                for (Rapporti r : rapportiList) {
                    zos.write((OutputFileFormatter.toRapportiOutputString(r)).getBytes(StandardCharsets.UTF_8));
                }

                // 3. Soggetti
                for (Collegamenti c : collegamentiList) {
                    zos.write((OutputFileFormatter.toCollegamentiOutputString(c)).getBytes(StandardCharsets.UTF_8));
                }

                // 4. Dati Contabili
                for (Collegamenti c : collegamentiList) {
                    zos.write((OutputFileFormatter.toCollegamentiOutputString2(c)).getBytes(StandardCharsets.UTF_8));
                }

                // 5. Footer
                String footer = OutputFileFormatter.createFooter(
                        rapportiList.size(),
                        collegamentiList.size(),
                        collegamentiList.size()
                );
                zos.write(footer.getBytes(StandardCharsets.UTF_8));

                zos.closeEntry();
            }
            zos.finish();

            // --- LOCAL TESTING BLOCK ---
            saveFileToResources(zipFileName, baos.toByteArray());

            // --- S3 UPLOAD (Commented out for testing) ---
        /*
        byte[] zipBytes = baos.toByteArray();
        String s3Key = outputFolder + "/" + zipFileName;
        try (ByteArrayInputStream zipInputStream = new ByteArrayInputStream(zipBytes)) {
            s3Service.uploadFile(s3Key, zipInputStream, "application/zip");
        }
        */
        }
    }

    private void saveFileToResources(String fileName, byte[] content) throws IOException {
        // Navigate to src/main/resources/output
        Path resourceDirectory = Paths.get("src", "main", "resources", "output");

        // Create directory if it doesn't exist
        if (!Files.exists(resourceDirectory)) {
            Files.createDirectories(resourceDirectory);
        }

        Path filePath = resourceDirectory.resolve(fileName);
        Files.write(filePath, content);

        log.info("Test file saved locally to: {}", filePath.toAbsolutePath());
    }

    private void uploadToS3(String fileName, byte[] content, Long submissionId) throws IOException {
        String s3Key = outputFolder + "/" + fileName;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(content)) {
            s3Service.uploadFile(s3Key, bais, "application/zip");
            log.info("Uploaded ZIP to S3: {} for submission {}", s3Key, submissionId);
        }
    }

    private String buildSubmissionPath(Submission submission) {
        if (submission == null || submission.getId() == null) {
            return "unknown";
        }

        // Ensure obligation is loaded (it's LAZY by default)
        Obligation obbligation = submission.getObligation();
        if (obbligation == null) {
            log.warn("Submission {} has no obligation, using submission ID only for path", submission.getId());
            return String.valueOf(submission.getId());
        }

        // Ensure period is loaded (it's LAZY by default)
        Period period = obbligation.getPeriod();
        Integer fiscalYear = obbligation.getFiscalYear();

        if (period == null || period.getName() == null || fiscalYear == null) {
            log.warn("Submission {} has incomplete obligation data (period={}, fiscalYear={}), using submission ID only for path",
                    submission.getId(), period != null ? period.getName() : "null", fiscalYear);
            return String.valueOf(submission.getId());
        }

        // Build path: SUBMISSION_ID_periodName_fiscalYear
        // Example: 174_112025
        return String.format("%d_%s%d", submission.getId(), period.getName(), fiscalYear);
    }

    private String normalizePath(String basePath, String subPath) {
        if (basePath == null) basePath = "";
        if (subPath == null) subPath = "";

        // Remove trailing slash from basePath
        basePath = basePath.replaceAll("/+$", "");
        // Remove leading slash from subPath
        subPath = subPath.replaceAll("^/+", "");

        if (basePath.isEmpty()) {
            return subPath;
        }
        if (subPath.isEmpty()) {
            return basePath;
        }

        return basePath + "/" + subPath;
    }

    private <T> List<List<T>> partitionList(List<T> list, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            batches.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        return batches;
    }
//
//    //    @Transactional(rollbackFor = Exception.class)
//    @Override
//    public void generateSubmissionOutputTxt(Long submissionId) throws NotFoundRecordException, IOException {
//        log.debug("Generating TXT outputs (ZIP) for submissionId={}", submissionId);
//
//        Submission submission = submissionRepository.findOneById(submissionId)
//                .orElseThrow(() -> new NotFoundRecordException("Submission not found with id: " + submissionId));
//
//        List<Output> outputs = generateOutputMerchant(submission);
//
//        if (submission.getOutputs() == null || submission.getOutputs().isEmpty()) {
//            throw new NotFoundRecordException("No outputs found for submission with id: " + submissionId);
//        }
//
//        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
//        String zipFileName = String.format("SUBMISSION_%d_OUTPUT_%s.zip", submissionId, timestamp);
////        int batchSize = 1000;
//
//        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
//             ZipOutputStream zos = new ZipOutputStream(baos, StandardCharsets.UTF_8)) {
//
//            for (Output output : outputs) {
//
//                List<Rapporti> rapportiList = rapportiRepository.findByOutputId(output.getId());
//                List<Soggetti> soggettiList = soggettiRepository.findByOutputId(output.getId());
//                List<DatiContabili> datiContabiliList = datiContabiliRepository.findByOutputId(output.getId());
//
//                log.debug("Processing outputId={} for submissionId={}", output.getId(), submissionId);
//
//                String header = OutputFileFormatter.createHeader();
//                String footer = OutputFileFormatter.createFooter(rapportiList.size(), soggettiList.size(), datiContabiliList.size());
//
////                int offset = 0;
//
//                String entryName = String.format(output.getFullPath(), output.getId());
//                zos.putNextEntry(new ZipEntry(entryName));
//                zos.write(header.getBytes(StandardCharsets.UTF_8));
//
//                if (!rapportiList.isEmpty()) {
//                    String rapportiContent = rapportiList.stream()
//                            .map(OutputFileFormatter::toRapportiOutputString)
//                            .collect(Collectors.joining("\n"));
//
//                    zos.write(rapportiContent.getBytes(StandardCharsets.UTF_8));
//                    zos.write("\n".getBytes(StandardCharsets.UTF_8));
//                }
//
//                if (!soggettiList.isEmpty()) {
//                    String soggettiContent = soggettiList.stream()
//                            .map(OutputFileFormatter::toSoggettiOutputString)
//                            .collect(Collectors.joining("\n"));
//
//                    zos.write(soggettiContent.getBytes(StandardCharsets.UTF_8));
//                    zos.write("\n".getBytes(StandardCharsets.UTF_8));
//                }
//
//                if (!datiContabiliList.isEmpty()) {
//                    String datiContabiliContent = datiContabiliList.stream()
//                            .map(OutputFileFormatter::toDatiContabiliOutputString)
//                            .collect(Collectors.joining("\n"));
//
//                    zos.write(datiContabiliContent.getBytes(StandardCharsets.UTF_8));
//                    zos.write("\n".getBytes(StandardCharsets.UTF_8));
//                }
//
////                while (true) {
////                    String bodyChunk;
////
////                    List<Collegamenti> batch = collegamentiRepository
////                            .findByOutputId(output.getId(), offset, batchSize);
////                    List<Rapporti> rapportiList = rapportiRepository
////                            .findByOutputId(output.getId(), offset, batchSize);
////                    List<DatiContabili> datiContabiliList = datiContabiliRepository
////                            .findByOutputId(output.getId(), offset, batchSize);
////                    List<Soggetti> soggettiList = soggettiRepository
////                            .findByOutputId(output.getId(), offset, batchSize);
////
////                    if (batch.isEmpty()) break;
////
////                    bodyChunk = batch.parallelStream()
////                            .map(OutputFileFormatter::toOutputFileString)
////                            .collect(Collectors.joining(""));
////
////                    offset += batch.size();
////                    zos.write(bodyChunk.getBytes(StandardCharsets.UTF_8));
////
////                    if (batch.size() < batchSize) break;
////                }
//
//
////                zos.write(footer.getBytes(StandardCharsets.UTF_8));
////                zos.closeEntry();
//            }
//            zos.finish();
//
//            // uncomment for local testing
////            File localFile = new File("C:\\My-Drive\\OKTA\\outputs\\" + zipFileName);
////            try (FileOutputStream fos = new FileOutputStream(localFile)) {
////                baos.writeTo(fos);
////            }
////            log.debug("ZIP file saved locally to: {}", localFile.getAbsolutePath());
//
//
//            byte[] zipBytes = baos.toByteArray();
//            String s3Key = outputFolder + "/" + zipFileName;
//
//            log.debug("Uploading ZIP file to S3: {} for submissionId={}", s3Key, submissionId);
//            try (ByteArrayInputStream zipInputStream = new ByteArrayInputStream(zipBytes)) {
//                String uploadedKey = s3Service.uploadFile(s3Key, zipInputStream, "application/zip");
//                log.debug("ZIP file successfully uploaded to S3: {} for submissionId={}", uploadedKey, submissionId);
//            }
//        }
//    }

    @Override
    @Async
    @Transactional(rollbackFor = Exception.class)
    public void generateSubmissionOutputTxtAsync(Long submissionId) {
        try {
            log.debug("Starting async output generation for submissionId={}", submissionId);
            generateSubmissionOutputTxt(submissionId);
            log.debug("Async output generation completed for submissionId={}", submissionId);
        } catch (NotFoundRecordException | IOException e) {
            log.error("Error generating output asynchronously for submissionId={}: {}", submissionId, e.getMessage(), e);
        }
    }

}