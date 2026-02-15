package it.deloitte.postrxade.service;

import it.deloitte.postrxade.dto.*;
import it.deloitte.postrxade.exception.NotFoundRecordException;

import java.util.List;

/**
 * Service interface for handling Analytical Insights and Visualization data.
 * <p>
 * This service provides the data contracts for various dashboard charts, including:
 * <ul>
 * <li>Transaction Summaries (Totals).</li>
 * <li>Historical Trend Breakdowns.</li>
 * <li>Sankey Diagrams (Flow visualization).</li>
 * <li>Payment Method Splits.</li>
 * </ul>
 */
public interface InsightsService {

    /**
     * Retrieves the high-level transaction summary for the current and previous period.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return {@link InsightsTransactionSummaryDTO} with aggregate totals.
     * @throws NotFoundRecordException If the current period data is corrupt.
     */
    InsightsTransactionSummaryDTO getTransactionsSummary(Integer fy, String period) throws NotFoundRecordException;

    /**
     * Retrieves a historical breakdown of transactions going back N periods.
     *
     * @param fy              The Fiscal Year.
     * @param period          The Period Name.
     * @param periodsToGoBack The number of historical periods to include.
     * @return List of {@link InsightsTransactionBreakdownDTO} for trend charts.
     * @throws NotFoundRecordException If the current period data is corrupt.
     */
    List<InsightsTransactionBreakdownDTO> getTransactionsBreakDown(Integer fy, String period, Integer periodsToGoBack) throws NotFoundRecordException;

    /**
     * Retrieves data formatted for a Sankey diagram, showing the flow of transactions
     * into Reportable/Non-Reportable categories and specific error types.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return {@link InsightsTransactionSankeyBreakdownDTO} with nodes and links.
     * @throws NotFoundRecordException If the current period data is corrupt.
     */
    InsightsTransactionSankeyBreakdownDTO getTransactionsSankeyBreakdown(Integer fy, String period) throws NotFoundRecordException;

    /**
     * Retrieves a detailed daily breakdown of payment volumes for the specified month.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return List of {@link InsightsPaymentBreakdownDTO}.
     * @throws NotFoundRecordException If the current period data is corrupt.
     */
    List<InsightsPaymentBreakdownDTO> getPaymentBreakDown(Integer fy, String period) throws NotFoundRecordException;

    /**
     * Retrieves the percentage split between different payment methods (e.g., POS vs E-Commerce).
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return List of {@link InsightsPaymentMethodSplitDTO}.
     * @throws NotFoundRecordException If the current period data is corrupt.
     */
    List<InsightsPaymentMethodSplitDTO> getPaymentMethodSplit(Integer fy, String period) throws NotFoundRecordException;
}