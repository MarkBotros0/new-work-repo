package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.dto.IngestionBatchDTO;
import it.deloitte.postrxade.dto.IngestionBreakdownDTO;
import it.deloitte.postrxade.dto.IngestionStatusDTO;
import it.deloitte.postrxade.dto.IngestionTransactionsDTO;
import it.deloitte.postrxade.entity.Ingestion;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.enums.IngestionStatusEnum;
import it.deloitte.postrxade.enums.IngestionTypeEnum;
import it.deloitte.postrxade.repository.*;
import it.deloitte.postrxade.service.IngestionDashboardService;
import ma.glasnost.orika.MapperFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Implementation of the IngestionDashboardService.
 * <p>
 * This service aggregates ingestion data from Submissions to populate the Ingestion Dashboard.
 * Unlike the validation service, this view focuses on the *technical* status of file uploads (Batches),
 * aggregating metrics like transaction counts, error counts, and overall batch status.
 */
@Service
public class IngestionDashboardServiceImpl implements IngestionDashboardService {

    @Autowired
    SubmissionRepository submissionRepository;
    @Autowired
    IngestionStatusRepository ingestionStatusRepository;
    @Autowired
    TransactionRepository transactionRepository;
    @Autowired
    SoggettiRepository soggettiRepository;
    @Autowired
    RapportiRepository rapportiRepository;
    @Autowired
    DatiContabiliRepository datiContabiliRepository;
    @Autowired
    CollegamentiRepository collegamentiRepository;
    @Autowired
    ErrorRecordRepository errorRecordRepository;
    @Autowired
    MerchantRepository merchantRepository;

    @Autowired
    private @Qualifier("alternativeMapperFacade") MapperFacade alternativeMapperFacade;

    /**
     * Retrieves all ingestion batches and calculates their aggregate status and statistics.
     * <p>
     * Logic:
     * 1. Fetches all submissions (including Cancelled/Rejected).
     * 2. Iterates through the Ingestions within each Submission.
     * 3. <strong>Aggregation:</strong> Sums up transaction and error counts for "transato" and "anagrafe" types.
     * 4. <strong>Status Determination:</strong> Calculates the overall batch status based on priority:
     * <ul>
     * <li>If ANY ingestion failed -> Batch is <strong>Failed</strong>.</li>
     * <li>If NO failures but ANY processing -> Batch is <strong>Processing</strong>.</li>
     * <li>If ALL are success -> Batch is <strong>Success</strong>.</li>
     * </ul>
     *
     * @return List of {@link IngestionBatchDTO} representing the dashboard rows.
     */
    @Override
    @Transactional(readOnly = true)
    public List<IngestionBatchDTO> getIngestionBatches() {

        List<Submission> submissions = submissionRepository.findAll();

        DateTimeFormatter PRETTY_FORMATTER = DateTimeFormatter
                .ofPattern("d MMMM yyyy, HH:mm", Locale.ENGLISH);

        return Optional.of(submissions)
                .orElse(Collections.emptyList())
                .stream()
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(Submission::getLastUpdatedAt).reversed())
                .map(submission -> {
                    IngestionBatchDTO dto = new IngestionBatchDTO();

                    if (submission.getBatchId() != null) {
                        dto.setBatchId(submission.getBatchId());
                    }

                    if (submission.getLastUpdatedAt() != null) {
                        dto.setUploadTime(PRETTY_FORMATTER.format(submission.getLastUpdatedAt()));
                    }

                    // --- Initialize Counters ---
                    long totalNoSoggetti = 0;
                    long totalNoSoggettiErrors = 0;
                    long totalNoRapporti = 0;
                    long totalNoRapportiErrors = 0;
                    long totalNoCollegamenti = 0;
                    long totalNoCollegamentiErrors = 0;
                    long totalNoDatiContabili = 0;
                    long totalNoDatiContabiliErrors = 0;

                    // --- Initialize Status Logic ---
                    // Default to UNKNOWN
                    String calculatedStatusName = IngestionStatusEnum.UNKNOWN.getLabel();
                    int currentPriority = IngestionStatusEnum.UNKNOWN.getPriority();

                    // Local variables for error capture
                    String foundErrorName = "";
                    String foundErrorDesc = "";

                    // --- Single Loop for Counts AND Status ---
                    if (submission.getIngestions() != null) {
                        for (Ingestion ingestion : submission.getIngestions()) {
                            if (ingestion == null) continue;

                            String typeName = (ingestion.getIngestionType() != null) ? ingestion.getIngestionType().getName() : "";
                            String statusName = (ingestion.getIngestionStatus() != null) ? ingestion.getIngestionStatus().getName() : "";

                            // A. COUNTING LOGIC (Using IngestionTypeEnum)
                            if (IngestionTypeEnum.SOGGETTI.getLabel().equals(typeName)) {
                                totalNoSoggetti += soggettiRepository.countByIngestionId(ingestion.getId());
                                totalNoSoggettiErrors += errorRecordRepository
                                        .countByIngestionIdAndIngestionIngestionTypeName(ingestion.getId(), IngestionTypeEnum.SOGGETTI.getLabel());
                            } else if (IngestionTypeEnum.RAPPORTI.getLabel().equals(typeName)) {
                                totalNoRapporti += rapportiRepository.countByIngestionId(ingestion.getId());
                                totalNoRapportiErrors += errorRecordRepository
                                        .countByIngestionIdAndIngestionIngestionTypeName(ingestion.getId(), IngestionTypeEnum.RAPPORTI.getLabel());
                            } else if (IngestionTypeEnum.DATI_CONTABILI.getLabel().equals(typeName)) {
                                totalNoDatiContabili += datiContabiliRepository.countByIngestionId(ingestion.getId());
                                totalNoDatiContabiliErrors += errorRecordRepository
                                        .countByIngestionIdAndIngestionIngestionTypeName(ingestion.getId(), IngestionTypeEnum.DATI_CONTABILI.getLabel());

                            } else if (IngestionTypeEnum.COLLEGAMENTI.getLabel().equals(typeName)) {
                                totalNoCollegamenti += collegamentiRepository.countByIngestionId(ingestion.getId());
                                totalNoCollegamentiErrors += errorRecordRepository
                                        .countByIngestionIdAndIngestionIngestionTypeName(ingestion.getId(), IngestionTypeEnum.COLLEGAMENTI.getLabel());

                            }

                            // B. STATUS LOGIC (Using IngestionStatusEnum)
                            // 1. Resolve Enum from string (Manual lookup or helper method)
                            IngestionStatusEnum currentEnum = Arrays.stream(IngestionStatusEnum.values())
                                    .filter(e -> e.getLabel().equalsIgnoreCase(statusName))
                                    .findFirst()
                                    .orElse(IngestionStatusEnum.UNKNOWN);

                            // 2. Check Priority (Higher priority overrides lower)
                            if (currentEnum.getPriority() > currentPriority) {
                                calculatedStatusName = currentEnum.getLabel();
                                currentPriority = currentEnum.getPriority();

                                // Capture error if Failed
                                if (currentEnum == IngestionStatusEnum.FAILED && ingestion.getIngestionError() != null) {
                                    foundErrorName = ingestion.getIngestionError().getName();
                                    foundErrorDesc = ingestion.getIngestionError().getDescription();
                                }
                            }
                        }
                    }

                    // --- Set Calculated Values ---

                    dto.setStatus(calculatedStatusName);

                    // Use Enum Labels for DTO keys to ensure frontend consistency
                    dto.setTransactions(Arrays.asList(
                            new IngestionTransactionsDTO(IngestionTypeEnum.SOGGETTI.getLabel(), String.valueOf(totalNoSoggetti + totalNoSoggettiErrors)),
                            new IngestionTransactionsDTO(IngestionTypeEnum.RAPPORTI.getLabel(), String.valueOf(totalNoRapporti + totalNoRapportiErrors)),
                            new IngestionTransactionsDTO(IngestionTypeEnum.DATI_CONTABILI.getLabel(), String.valueOf(totalNoDatiContabili + totalNoDatiContabiliErrors)),
                            new IngestionTransactionsDTO(IngestionTypeEnum.COLLEGAMENTI.getLabel(), String.valueOf(totalNoCollegamenti + totalNoCollegamentiErrors))
                    ));

                    dto.setBreakdown(Arrays.asList(
                            new IngestionBreakdownDTO(IngestionTypeEnum.SOGGETTI.getLabel(), String.valueOf(totalNoSoggetti), String.valueOf(totalNoSoggettiErrors)),
                            new IngestionBreakdownDTO(IngestionTypeEnum.RAPPORTI.getLabel(), String.valueOf(totalNoRapporti), String.valueOf(totalNoRapportiErrors)),
                            new IngestionBreakdownDTO(IngestionTypeEnum.DATI_CONTABILI.getLabel(), String.valueOf(totalNoDatiContabili), String.valueOf(totalNoDatiContabiliErrors)),
                            new IngestionBreakdownDTO(IngestionTypeEnum.COLLEGAMENTI.getLabel(), String.valueOf(totalNoCollegamenti), String.valueOf(totalNoCollegamentiErrors))
                    ));

                    // Set Month
                    if (submission.getObligation() != null &&
                            submission.getObligation().getPeriod() != null) {
                        dto.setMonth(submission.getObligation().getPeriod().getName());
                    } else {
                        dto.setMonth(IngestionStatusEnum.UNKNOWN.getLabel());
                    }

                    // Set Error Details
                    dto.setError(foundErrorName);
                    dto.setMessage(foundErrorDesc);

                    return dto;
                })
                .toList();
    }

    /**
     * Retrieves all available ingestion statuses from the database.
     *
     * @return List of {@link IngestionStatusDTO}.
     */
    @Override
    @Transactional(readOnly = true)
    public List<IngestionStatusDTO> getIngestionStatuses() {
        return alternativeMapperFacade.mapAsList(ingestionStatusRepository.findAll(), IngestionStatusDTO.class);
    }
}
