package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.dto.DashboardStatsDTO;
import it.deloitte.postrxade.entity.Ingestion;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.enums.IngestionStatusEnum;
import it.deloitte.postrxade.enums.IngestionTypeEnum;
import it.deloitte.postrxade.enums.SeverityEnum;
import it.deloitte.postrxade.enums.SubmissionStatusEnum;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.repository.*;
import it.deloitte.postrxade.service.ObligationService;
import it.deloitte.postrxade.service.OverviewDashboardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of the DashboardService.
 * <p>
 * This service aggregates data from the {@link ObligationService} to populate the
 * main dashboard. It handles the logic for:
 * <ul>
 * <li>Fetching current and previous period submissions.</li>
 * <li>Calculating ingestion success/failure rates.</li>
 * <li>Aggregating transaction volume insights.</li>
 * <li>Summarizing validation issues (Errors vs Warnings).</li>
 * <li>Tracking obligation statuses (Completed, Pending, Cancelled).</li>
 * </ul>
 */
@Service
@Transactional
public class OverviewDashboardServiceImpl implements OverviewDashboardService {

    @Autowired
    private ObligationService obligationService;
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
    private CambioNdgRepository cambioNdgRepository;
    @Autowired
    private ErrorRecordRepository errorRecordRepository;
    @Autowired
    private ErrorCauseRepository errorCauseRepository;


    /**
     * Retrieves the aggregated dashboard statistics for a given Fiscal Year and Period.
     *
     * @param period     The specific period string (e.g., "January").
     * @param fiscalYear The fiscal year (e.g., 2023).
     * @return A {@link DashboardStatsDTO} containing all calculated metrics.
     * @throws NotFoundRecordException If the current period data is in an invalid state (e.g., multiple active submissions).
     */
    @Override
    public DashboardStatsDTO getDashboardStats(String period, Integer fiscalYear) throws NotFoundRecordException {

        List<Submission> submissions = obligationService.getSubmissionsForStats(fiscalYear, period);

        List<Submission> preSubmissions;
        try {
            preSubmissions = obligationService.getSubmissionsFromPastPeriod(fiscalYear, period, 1);
        } catch (NotFoundRecordException e) {
            // Explicitly ensure 0s for past data, even if corrupt
            preSubmissions = Collections.emptyList();
        }

        // 2. Initialize DTO
        DashboardStatsDTO stats = new DashboardStatsDTO();

        // 3. Single-Pass Aggregation for Current Period
        aggregateCurrentPeriodStats(submissions, stats);

        // 4. Single-Pass Aggregation for Previous Period
//        aggregatePastPeriodStats(preSubmissions, stats);
        return stats;
    }

    /**
     * Optimized method that calculates Obligation, Ingestion, Validation, and Transaction stats
     * in a SINGLE iteration over the list.
     */
    private void aggregateCurrentPeriodStats(List<Submission> submissions, DashboardStatsDTO stats) {
        int registeredAbandoned = 0;
        int approved = 0;
        int completed = 0;

        int ingFailed = 0;
        int ingProcessing = 0;
        int ingSuccess = 0;

        long soggettiReceived = 0;
        long soggettiAccepted = 0;
        long soggettiErrors = 0;
        long rapportiReceived = 0;
        long rapportiAccepted = 0;
        long rapportiErrors = 0;
        long collegamentiReceived = 0;
        long collegamentiAccepted = 0;
        long collegamentiErrors = 0;
        long datiContabiliReceived = 0;
        long datiContabiliAccepted = 0;
        long datiContabiliErrors = 0;

        long totalErrors = 0;
        long totalErrorRecords = 0;
        long totalWarnings = 0;

        for (Submission submission : submissions) {
            String subStatus = submission.getCurrentSubmissionStatus().getName();

            if (SubmissionStatusEnum.CANCELLED.getDbName().equals(subStatus)
                    || SubmissionStatusEnum.ERROR.getDbName().equals(subStatus)) {
                registeredAbandoned++;
            } else if (SubmissionStatusEnum.PENDING_SUBMISSION.getDbName().equals(subStatus)) {
                approved++;
            } else if (SubmissionStatusEnum.COMPLETED.getDbName().equals(subStatus)) {
                completed++;
            }

            if (!subStatus.equals(SubmissionStatusEnum.CANCELLED.getDbName())
                    && !subStatus.equals(SubmissionStatusEnum.REJECTED.getDbName())
                    && !subStatus.equals(SubmissionStatusEnum.ERROR.getDbName())) {

                List<Ingestion> ingestions = submission.getIngestions();

                if (ingestions != null) {
                    for (Ingestion ingestion : ingestions) {

                        String ingStatus = ingestion.getIngestionStatus().getName();
                        if (IngestionStatusEnum.FAILED.getLabel().equals(ingStatus)) {
                            ingFailed++;
                        } else if (IngestionStatusEnum.PROCESSING.getLabel().equals(ingStatus)) {
                            ingProcessing++;
                        } else {
                            ingSuccess++;
                        }

                        if (ingestion.getIngestionType().getName().equals(IngestionTypeEnum.SOGGETTI.getLabel())) {
                            soggettiAccepted += soggettiRepository.countByIngestionId(ingestion.getId());
                            soggettiErrors += errorRecordRepository.countErrorRecordsByIngestionIdAndIngestionTypeName(
                                    ingestion.getId(), IngestionTypeEnum.SOGGETTI.getLabel());
                            totalWarnings += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.WARNING.getLevel());
                            totalErrors += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.ERROR.getLevel());
                        }

                        if (ingestion.getIngestionType().getName().equals(IngestionTypeEnum.RAPPORTI.getLabel())) {
                            rapportiAccepted += rapportiRepository.countByIngestionId(ingestion.getId());
                            rapportiErrors += errorRecordRepository.countErrorRecordsByIngestionIdAndIngestionTypeName(
                                    ingestion.getId(), IngestionTypeEnum.RAPPORTI.getLabel());
                            totalWarnings += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.WARNING.getLevel());
                            totalErrors += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.ERROR.getLevel());
                        }

                        if (ingestion.getIngestionType().getName().equals(IngestionTypeEnum.DATI_CONTABILI.getLabel())) {
                            datiContabiliAccepted += datiContabiliRepository.countByIngestionId(ingestion.getId());
                            datiContabiliErrors += errorRecordRepository.countErrorRecordsByIngestionIdAndIngestionTypeName(
                                    ingestion.getId(), IngestionTypeEnum.DATI_CONTABILI.getLabel());
                            totalWarnings += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.WARNING.getLevel());
                            totalErrors += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.ERROR.getLevel());
                        }

                        if (ingestion.getIngestionType().getName().equals(IngestionTypeEnum.COLLEGAMENTI.getLabel())) {
                            collegamentiAccepted += collegamentiRepository.countByIngestionId(ingestion.getId());
                            collegamentiErrors += errorRecordRepository.countErrorRecordsByIngestionIdAndIngestionTypeName(
                                    ingestion.getId(), IngestionTypeEnum.COLLEGAMENTI.getLabel());
                            totalWarnings += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.WARNING.getLevel());
                            totalErrors += errorCauseRepository.countByIngestionAndSeverity(ingestion.getId(), SeverityEnum.ERROR.getLevel());
                        }
                    }
                }
            }
        }

        stats.setObligationTotal(submissions.size());
        stats.setObligationRejectedAbandoned(registeredAbandoned);
        stats.setObligationApproved(approved);
        stats.setObligationCompleted(completed);

        stats.setFailed(ingFailed);
        stats.setProcessing(ingProcessing);
        stats.setSuccess(ingSuccess);

        stats.setSoggettiAccepted(soggettiAccepted);
        stats.setSoggettiReceived(soggettiReceived + soggettiErrors);
        stats.setSoggettiAccepted(soggettiAccepted);
        stats.setSoggettiReceived(soggettiReceived + soggettiErrors);
        stats.setSoggettiAccepted(soggettiAccepted);
        stats.setSoggettiReceived(soggettiReceived + soggettiErrors);
        stats.setSoggettiAccepted(soggettiAccepted);
        stats.setSoggettiReceived(soggettiReceived + soggettiErrors);

        stats.setError(totalErrors);
        stats.setWarning(totalWarnings);
    }

    /**
     * Optimized method for Past Period (only needs Transaction stats).
     */
//    private void aggregatePastPeriodStats(List<Submission> submissions, DashboardStatsDTO stats) {
//        List<Long> submissionIds = submissions.stream().map(Submission::getId).toList();
//        long totalTrans = transactionRepository.countTransactionsBySubmissionIds(submissionIds);
//        long totalErrors = errorRecordRepository.countErrorRecordsBySubmissionIds(submissionIds);
//
//        stats.setTotalPreviousReportableTransactions(totalTrans);
//        stats.setTotalPreviousTransactions(totalTrans + totalErrors);
//    }


}
