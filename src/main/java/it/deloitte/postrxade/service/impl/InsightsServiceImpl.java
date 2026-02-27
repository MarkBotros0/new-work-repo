package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.dto.*;
import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.enums.IngestionTypeEnum;
import it.deloitte.postrxade.enums.PaymentTypeEnum;
import it.deloitte.postrxade.enums.SubmissionStatusEnum;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.records.TransactionDateCount;
import it.deloitte.postrxade.repository.*;
import it.deloitte.postrxade.service.InsightsService;
import it.deloitte.postrxade.service.ObligationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of the InsightsService.
 * <p>
 * This service handles the analytical logic for the application.
 * It interacts with the {@link ObligationService} to retrieve clean data (handling strict integrity checks)
 * and then transforms that data into various DTOs for charts and summaries (Sankey, Breakdown, Payment Methods, etc.).
 */
@Service
public class InsightsServiceImpl implements InsightsService {

    private static final List<String> KNOWN_PAYMENT_TYPES = List.of("E-commerce", "POS");

    @Autowired
    private ObligationService obligationService;
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
    private CollegamentiRepository collegamentiRepository;
    @Autowired
    private DatiContabiliRepository datiContabiliRepository;
    @Autowired
    private ErrorRecordRepository errorRecordRepository;
    @Autowired
    private ErrorCauseRepository errorCauseRepository;
    @Autowired
    private ErrorTypeRepository errorTypeRepository;

    /**
     * Retrieves a high-level summary of transactions for the current period and the previous period.
     * <p>
     * Logic:
     * 1. Fetches current period data strictly (throws error if corrupt).
     * 2. Fetches previous period data leniency (returns empty if corrupt).
     * 3. Aggregates totals for both periods to allow comparison in the UI.
     *
     * @param fiscalYear The Fiscal Year.
     * @param periodName     The Period Name.
     * @return {@link InsightsTransactionSummaryDTO} with current and past totals.
     * @throws NotFoundRecordException if current period data is corrupt (>1 active submission).
     */
    @Override
    @Transactional(readOnly = true)
    public InsightsTransactionSummaryDTO getTransactionsSummary(Integer fiscalYear, String periodName) throws NotFoundRecordException {
//        Optional<Submission> activeSubmissionOpt = obligationService.getActiveSubmissionForStats(fiscalYear, period);

        Period period = periodRepository.findByName(periodName).orElse(null);
        ;
        Obligation obligation = null;

        if (period != null) {
            obligation = obligationRepository.findByFiscalYearAndPeriod(fiscalYear, period).orElse(null);
        }

        List<Submission> submissions = new ArrayList<>();

        if (obligation != null) {
            submissions = obligation.getSubmissions().stream().filter(s -> {
                return !s.getCurrentSubmissionStatus().getOrder().equals(SubmissionStatusEnum.ERROR.getOrder()) &&
                        !s.getCurrentSubmissionStatus().getOrder().equals(SubmissionStatusEnum.CANCELLED.getOrder()) &&
                        !s.getCurrentSubmissionStatus().getOrder().equals(SubmissionStatusEnum.REJECTED.getOrder());
            }).toList();

        }

        List<Submission> preSubmissions;
        try {
            preSubmissions = obligationService.getSubmissionsFromPastPeriod(fiscalYear, periodName, 1);
        } catch (NotFoundRecordException e) {
            preSubmissions = Collections.emptyList();
        }

        InsightsTransactionSummaryDTO insightsPageDTO = new InsightsTransactionSummaryDTO();

        getInsightsStats(submissions, preSubmissions, insightsPageDTO);

        return insightsPageDTO;
    }


    /**
     * Generates a historical breakdown of transaction stats going back N periods.
     * <p>
     * Logic:
     * Iterates backwards from the current period. For each period:
     * - If it's the requested current period, enforce strict integrity (throw error on corruption).
     * - If it's a past period, handle corruption gracefully (treat as empty/0s).
     *
     * @param fy              The starting Fiscal Year.
     * @param period          The starting Period Name.
     * @param periodsToGoBack Number of historical periods to include.
     * @return List of {@link InsightsTransactionBreakdownDTO} for trend charts.
     * @throws NotFoundRecordException if the *current* period has corrupt data.
     */
    @Override
    @Transactional(readOnly = true)
    public List<InsightsTransactionBreakdownDTO> getTransactionsBreakDown(Integer fy, String period, Integer periodsToGoBack) throws NotFoundRecordException {

        List<InsightsTransactionBreakdownDTO> insightsPageDTOs = new ArrayList<>();
        List<PeriodSubmissionData> periodDataList = new ArrayList<>();

        periodDataList.add(obligationService.getDataByFyAndPeriod(fy, period));
        for (int j = 1; j <= periodsToGoBack; j++) {
            periodDataList.add(obligationService.getDataFromPastPeriod(fy, period, j));
        }

        for (PeriodSubmissionData periodData : periodDataList) {
            InsightsTransactionBreakdownDTO dto = new InsightsTransactionBreakdownDTO();

            dto.setPeriod(periodData.periodName());
            dto.setFiscalYear(periodData.fiscalYear());

            List<Submission> activeSubmissions;

            if (periodData.obligation() != null) {
                activeSubmissions = obligationService.getActiveSubmissions(periodData.obligation());
            } else {
                activeSubmissions = Collections.emptyList();
            }
            List<Submission> submissionsToCalculate;

            if (activeSubmissions.size() == 1) {
                submissionsToCalculate = activeSubmissions;
            } else if (activeSubmissions.size() > 1) {
                boolean isCurrentPeriod = periodData.fiscalYear().equals(fy) &&
                        periodData.periodName().equals(period);

                if (isCurrentPeriod) {
                    throw new NotFoundRecordException(
                            String.format("Data corruption: Found %d active submissions for current period %s %d",
                                    activeSubmissions.size(), period, fy)
                    );
                }
                submissionsToCalculate = Collections.emptyList();
            } else {
                submissionsToCalculate = Collections.emptyList();
            }
            getInsightsStats(submissionsToCalculate, dto);
            insightsPageDTOs.add(dto);
        }
        return insightsPageDTOs;
    }

//    /**
//     * If periodsToGoBack is n then the data will be about n+1 periods
//     */
//    @Override
//    @Transactional(readOnly = true)
//    public List<InsightsTransactionBreakdownDTO> getTransactionsBreakDown(Integer fy, String period, Integer periodsToGoBack)   {
//
//        List<InsightsTransactionBreakdownDTO> insightsPageDTOs = new ArrayList<>();
//
//        List<List<Submission>>  listOfSubmissionLists = new ArrayList<>();
//        listOfSubmissionLists.add(obligationService.getAllSubmissionByFyAndPeriod(fy, period));
//        for(int j=1;j<=periodsToGoBack;j++){
//            listOfSubmissionLists.add(obligationService.getSubmissionsFromPastPeriod(fy, period,j));
//        }
//
//        for (List<Submission> submissionList : listOfSubmissionLists) {
//            InsightsTransactionBreakdownDTO dto = new InsightsTransactionBreakdownDTO();
//            if(!submissionList.isEmpty()){
//                dto.setPeriod(submissionList.getFirst().getObligation().getPeriod().getName());
//                dto.setFiscalYear(submissionList.getFirst().getObligation().getFiscalYear());
//            }
//            getInsightsStats(submissionList, dto);
//            insightsPageDTOs.add(dto);
//        }
//
//        return insightsPageDTOs;
//    }

    /**
     * Prepares data for a Sankey Diagram visualization.
     * <p>
     * Flows mapped:
     * 1. Total Transactions -> Reportable vs Non-Reportable (Errors).
     * 2. Non-Reportable -> Breakdown by specific Error Types.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return {@link InsightsTransactionSankeyBreakdownDTO} containing nodes and links.
     * @throws NotFoundRecordException if current period data is corrupt.
     */
    @Override
    @Transactional(readOnly = true)
    public InsightsTransactionSankeyBreakdownDTO getTransactionsSankeyBreakdown(Integer fy, String period, String type) throws NotFoundRecordException {
        Optional<Submission> activeSubmissionOpt = obligationService.getActiveSubmissionForStats(fy, period);

        List<Submission> activeSubmissions = activeSubmissionOpt
                .map(List::of)
                .orElse(Collections.emptyList());

        List<Long> submissionIds = activeSubmissions.stream().map(Submission::getId).toList();

        InsightsTransactionSankeyBreakdownDTO dto = new InsightsTransactionSankeyBreakdownDTO();

        // Map type parameter to IngestionTypeEnum
        String ingestionTypeLabel = type.toUpperCase();
        IngestionTypeEnum ingestionType;
        try {
            ingestionType = IngestionTypeEnum.valueOf(ingestionTypeLabel);
        } catch (IllegalArgumentException e) {
            ingestionType = IngestionTypeEnum.SOGGETTI; // Default to SOGGETTI
        }

        Map<String, Long> errorCounts = new HashMap<>();
        long totalNoErrorTransactions = transactionRepository.countTransactionsBySubmissionIds(submissionIds);
        long errorRecords = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(submissionIds, ingestionType.getLabel());

        List<ErrorType> distinctErrorTypes = errorTypeRepository.findDistinctErrorTypesBySubmissionIdsAndIngestionType(
                submissionIds, ingestionType.getLabel());

        for (ErrorType errorType : distinctErrorTypes) {
            Long errorCount = errorCauseRepository.countBySubmissionsAndErrorType(submissionIds, errorType.getId());
            errorCounts.put(errorType.getName(), errorCount);
        }

        long totalTransactions = totalNoErrorTransactions + errorRecords;

        int lastNode = 0;
        dto.addItem("Records", 0, -1, totalTransactions);

        if (errorRecords != 0) {
            dto.addItem("Non Reportable", 0, 1, errorRecords);
            lastNode = 1;
        }
        if (totalNoErrorTransactions != 0) {
            if (errorRecords != 0) {
                dto.addItem("Reportable", 0, 2, totalNoErrorTransactions);
                dto.addItem("Reportable", 2, 3, totalNoErrorTransactions);
                lastNode = 3;
            } else {
                dto.addItem("Reportable", 0, 1, totalNoErrorTransactions);
                dto.addItem("Reportable", 1, 2, totalNoErrorTransactions);
                lastNode = 2;
            }
        }

        int i = lastNode + 1;
        for (Map.Entry<String, Long> entry : errorCounts.entrySet()) {
            dto.addItem(entry.getKey(), 1, i++, entry.getValue());
        }

        return dto;
    }

    /**
     * Provides a daily breakdown of payment volume for the specified month.
     * <p>
     * Logic:
     * 1. Determines the full date range for the month.
     * 2. Aggregates transactions by `dtOpe` (date of operation).
     * 3. Fills in missing days with 0s to ensure a complete chart x-axis.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return List of {@link InsightsPaymentBreakdownDTO} per day.
     * @throws NotFoundRecordException if current period data is corrupt.
     */
    @Override
    @Transactional(readOnly = true)
    public List<InsightsPaymentBreakdownDTO> getPaymentBreakDown(Integer fy, String period) throws NotFoundRecordException {

        Optional<Period> optionalPeriod = periodRepository.findByName(period);

        if (optionalPeriod.isEmpty()) {
            return Collections.emptyList();
        }

        int monthOrder = optionalPeriod.get().getOrder(); // Assuming 1 = January, 12 = December
        YearMonth targetYearMonth;

        try {
            targetYearMonth = YearMonth.of(fy, monthOrder);
        } catch (Exception e) {
            return Collections.emptyList();
        }

        Optional<Submission> activeSubmissionOpt = obligationService.getActiveSubmissionForStats(fy, period);

        List<Submission> activeSubmissions = activeSubmissionOpt
                .map(List::of)
                .orElse(Collections.emptyList());

        List<Long> submissionIds = activeSubmissions.stream().map(Submission::getId).toList();

        List<TransactionDateCount> transactionDateCounts = transactionRepository.countTransactionsGroupedByDate(submissionIds);

        Map<LocalDate, Long> countsByDate = transactionDateCounts.stream()
                .collect(Collectors.toMap(
                        TransactionDateCount::date,
                        TransactionDateCount::count
                ));


        LocalDate startDate = targetYearMonth.atDay(1);
        LocalDate firstDayOfNextMonth = targetYearMonth.plusMonths(1).atDay(1);

        return startDate.datesUntil(firstDayOfNextMonth)
                .map(date -> {
                    long count = countsByDate.getOrDefault(date, 0L);
                    return new InsightsPaymentBreakdownDTO(
                            date.getDayOfMonth(),
                            count,
                            date,
                            "TransactionsPerDayResponse"
                    );
                })
                .toList();
    }

    /**
     * Calculates the percentage split between Payment Types (e.g., E-commerce vs POS).
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return List of {@link InsightsPaymentMethodSplitDTO} with percentages.
     * @throws NotFoundRecordException if current period data is corrupt.
     */
    @Override
    @Transactional(readOnly = true)
    public List<InsightsPaymentMethodSplitDTO> getPaymentMethodSplit(Integer fy, String period) throws NotFoundRecordException {

        Optional<Submission> activeSubmissionOpt = obligationService.getActiveSubmissionForStats(fy, period);
        List<Submission> activeSubmissions = activeSubmissionOpt
                .map(List::of)
                .orElse(Collections.emptyList());
        List<Long> submissionIds = activeSubmissions.stream().map(Submission::getId).toList();

        List<PaymentTypeEnum> chartTypes = List.of(
                PaymentTypeEnum.E_COMMERCE,
                PaymentTypeEnum.POS);

        Map<String, Long> actualCounts = new HashMap<>();

        for (PaymentTypeEnum type : chartTypes) {
            Long transactionsCount = transactionRepository
                    .countTransactionsByObligationAndTipoPag(submissionIds, type.getCode());
            actualCounts.put(type.getCode(), transactionsCount);
        }

        long totalCount = actualCounts.values().stream().mapToLong(Long::longValue).sum();

        List<InsightsPaymentMethodSplitDTO> dtoList = new ArrayList<>();

        for (PaymentTypeEnum type : chartTypes) {
            long count = actualCounts.getOrDefault(type.getCode(), 0L);
            double percentage = (totalCount == 0) ? 0.0 : ((double) count / totalCount) * 100.0;

            dtoList.add(new InsightsPaymentMethodSplitDTO(
                    percentage,
                    type.getLabel(),
                    "TransactionTypeGraph"
            ));
        }

        return dtoList;
    }

    // ==================================================================================
    // PRIVATE HELPER METHODS
    // ==================================================================================

    /**
     * Safely parses a YYYYMMDD integer date format into a {@link LocalDate}.
     *
     * @param dtOpe The date integer (e.g., 20240101).
     * @return The LocalDate object or null if parsing fails.
     */
    private LocalDate parseDateFromInt(String dtOpe) {
        // Define the formatter here
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ddMMyyyy");

        if (dtOpe.length() != 8) {
            return null; // Or log and return null
        }
        try {
            return LocalDate.parse(dtOpe, formatter);
        } catch (DateTimeParseException e) {
            return null; // Or log and return null
        }
    }

    /**
     * Helper to populate the summary DTO with transaction counts for current and previous periods.
     */
    private void getInsightsStats(List<Submission> submissions, List<Submission> prevSubmissions, InsightsTransactionSummaryDTO stats) {
        List<Long> submissoinIds = submissions.stream().map(Submission::getId).toList();
        List<Long> prevSubmissoinIds = prevSubmissions.stream().map(Submission::getId).toList();

        long totalNoErrorSoggetti = soggettiRepository.countBySubmissionIn(submissoinIds);
        long totalNoErrorRapporti = rapportiRepository.countBySubmissionIn(submissoinIds);
        long totalNoErrorDatiContabili = datiContabiliRepository.countBySubmissionIn(submissoinIds);
        long totalNoErrorCollegamenti = collegamentiRepository.countBySubmissionIn(submissoinIds);

        long totalPrevNoErrorSoggetti = soggettiRepository.countBySubmissionIn(prevSubmissoinIds);
        long totalPrevNoErrorRapporti = rapportiRepository.countBySubmissionIn(prevSubmissoinIds);
        long totalPrevNoErrorDatiContabili = datiContabiliRepository.countBySubmissionIn(prevSubmissoinIds);
        long totalPrevNoErrorCollegamenti = collegamentiRepository.countBySubmissionIn(prevSubmissoinIds);

        long soggetiErrors = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(submissoinIds, IngestionTypeEnum.SOGGETTI.getLabel());
        long rapportiErrors = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(submissoinIds, IngestionTypeEnum.RAPPORTI.getLabel());
        long datiCollegamentiErrors = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(submissoinIds, IngestionTypeEnum.DATI_CONTABILI.getLabel());
        long collegamentiErrors = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(submissoinIds, IngestionTypeEnum.COLLEGAMENTI.getLabel());

        long soggetiPrevErrors = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(prevSubmissoinIds, IngestionTypeEnum.SOGGETTI.getLabel());
        long rapportiPrevErrors = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(prevSubmissoinIds, IngestionTypeEnum.RAPPORTI.getLabel());
        long datiContabiliPrevErrors = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(prevSubmissoinIds, IngestionTypeEnum.DATI_CONTABILI.getLabel());
        long collegamentiPrevErrors = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(prevSubmissoinIds, IngestionTypeEnum.COLLEGAMENTI.getLabel());

        stats.setSoggettiAccepted(totalNoErrorSoggetti);
        stats.setSoggettiReceived(totalNoErrorSoggetti + soggetiErrors);
        stats.setPreviousSoggettiAccepted(totalPrevNoErrorSoggetti);
        stats.setPreviousSoggettiReceived(totalPrevNoErrorSoggetti + soggetiPrevErrors);

        stats.setRapportiAccepted(totalNoErrorRapporti);
        stats.setRapportiReceived(totalNoErrorRapporti + rapportiErrors);
        stats.setPreviousRapportiAccepted(totalPrevNoErrorRapporti);
        stats.setPreviousRapportiReceived(totalPrevNoErrorRapporti + rapportiPrevErrors);

        stats.setDatiContabiliAccepted(totalNoErrorDatiContabili);
        stats.setDatiContabiliReceived(totalNoErrorDatiContabili + datiCollegamentiErrors);
        stats.setPreviousDatiContabiliAccepted(totalPrevNoErrorDatiContabili);
        stats.setPreviousDatiContabiliReceived(totalPrevNoErrorDatiContabili + datiContabiliPrevErrors);

        stats.setCollegamentiAccepted(totalNoErrorCollegamenti);
        stats.setCollegamentiReceived(totalNoErrorCollegamenti + collegamentiErrors);
        stats.setPreviousCollegamentiAccepted(totalPrevNoErrorCollegamenti);
        stats.setPreviousCollegamentiReceived(totalPrevNoErrorCollegamenti + collegamentiPrevErrors);
    }

    /**
     * Helper to populate the breakdown DTO for a specific period (Current).
     */
    private void getInsightsStats(List<Submission> submissions, InsightsTransactionBreakdownDTO stats) {
        List<Long> submissionIds = submissions.stream().map(Submission::getId).toList();

        long totalNoErrorTransactions = transactionRepository.countTransactionsBySubmissionIds(submissionIds);
        long errors = errorRecordRepository.countErrorRecordsBySubmissionIdsAndIngestionType(
                submissionIds, IngestionTypeEnum.SOGGETTI.getLabel());

        long totalTransactions = totalNoErrorTransactions + errors;

        stats.setTotalTransactions(totalTransactions);
        stats.setTotalReportableTransactions(totalNoErrorTransactions);
    }
}
