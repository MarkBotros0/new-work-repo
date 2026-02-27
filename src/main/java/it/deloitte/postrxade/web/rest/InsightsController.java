package it.deloitte.postrxade.web.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import it.deloitte.postrxade.dto.*;
import it.deloitte.postrxade.enums.AuthIdProfilo;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.security.RequireAuthorities;
import it.deloitte.postrxade.service.InsightsService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST controller for serving Insights and Statistical data.
 * <p>
 * This controller provides endpoints for various analytical views, including:
 * <ul>
 * <li>High-level transaction summaries.</li>
 * <li>Historical breakdowns for trend analysis.</li>
 * <li>Payment method splits and detailed payment breakdowns.</li>
 * <li>Sankey diagram data for flow visualization.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/insights/stats")
@CrossOrigin("*")
@Tag(name = "Insights Endpoint", description = "Endpoints for retrieving Analytical Insights and Charts")
public class InsightsController {

    private final InsightsService insightsService;

    /**
     * Constructor-based dependency injection.
     *
     * @param insightsService The service responsible for calculating insights.
     */
    public InsightsController(InsightsService insightsService) {
        this.insightsService = insightsService;
    }

    /**
     * Retrieves the high-level transaction summary (Counts and Totals).
     * <p>
     * Endpoint: GET /api/insights/stats/transactions/count
     *
     * @param fy     The Fiscal Year.
     * @param period The specific month name.
     * @return A {@link ResponseEntity} containing the {@link InsightsTransactionSummaryDTO}.
     * @throws NotFoundRecordException if data is missing.
     */
    @GetMapping("/transactions/count")
    @Operation(summary = "Get Insights overview", description = "Request to get Insights count by given FY and Period")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<InsightsTransactionSummaryDTO> getTransactionsInsights(
            @RequestParam("fy") Integer fy,
            @RequestParam("period") String period) throws NotFoundRecordException {
        InsightsTransactionSummaryDTO insightsData = insightsService.getTransactionsSummary(fy, period);
        return new ResponseEntity<>(insightsData, HttpStatus.OK);
    }

    /**
     * Retrieves the historical transaction breakdown for charts.
     * <p>
     * Endpoint: GET /api/insights/stats/transactions/breakdown
     * <p>
     * Note: If an obligation has more than one "ACTIVE" submission, this is considered a data error,
     * and the service may return 0s or handle it gracefully depending on implementation.
     *
     * @param fy              The Fiscal Year.
     * @param period          The specific month name.
     * @param periodsToGoBack The number of past periods to include in the breakdown.
     * @return A list of {@link InsightsTransactionBreakdownDTO} for trend analysis.
     */
    @GetMapping("/transactions/breakdown")
    @Operation(summary = "Get Insights breakdown chart", description = "Request to get Insights breakdown chart by given FY and Period")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<List<InsightsTransactionBreakdownDTO>> getPaymentInsights(
            @RequestParam("fy") Integer fy,
            @RequestParam("period") String period,
            @RequestParam("periodsToGoBack") Integer periodsToGoBack) throws NotFoundRecordException {
        List<InsightsTransactionBreakdownDTO> insightsData = insightsService.getTransactionsBreakDown(fy, period, periodsToGoBack);
        return new ResponseEntity<>(insightsData, HttpStatus.OK);
    }

    /**
     * Retrieves a detailed breakdown of payments for the current period.
     * <p>
     * Endpoint: GET /api/insights/stats/transactions/payment/breakdown
     *
     * @param fy     The Fiscal Year.
     * @param period The specific month name.
     * @return A list of {@link InsightsPaymentBreakdownDTO}.
     */
    @GetMapping("/transactions/payment/breakdown")
    @Operation(summary = "Get Insights payment breakdown", description = "Request to get detailed payment breakdown by given FY and Period")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<List<InsightsPaymentBreakdownDTO>> getPaymentBreakdownInsights(
            @RequestParam("fy") Integer fy,
            @RequestParam("period") String period) throws NotFoundRecordException {
        List<InsightsPaymentBreakdownDTO> insightsData = insightsService.getPaymentBreakDown(fy, period);
        return new ResponseEntity<>(insightsData, HttpStatus.OK);
    }

    /**
     * Retrieves the distribution of payment methods (e.g., Check, Wire, Card).
     * <p>
     * Endpoint: GET /api/insights/stats/transactions/payment/methods
     *
     * @param fy     The Fiscal Year.
     * @param period The specific month name.
     * @return A list of {@link InsightsPaymentMethodSplitDTO} representing the split.
     */
    @GetMapping("/transactions/payment/methods")
    @Operation(summary = "Get Insights Payment Method Split chart", description = "Request to get Insights Payment Method Split chart by given FY and Period")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<List<InsightsPaymentMethodSplitDTO>> getPaymentMethodInsights(
            @RequestParam("fy") Integer fy,
            @RequestParam("period") String period) throws NotFoundRecordException {
        List<InsightsPaymentMethodSplitDTO> insightsData = insightsService.getPaymentMethodSplit(fy, period);
        return new ResponseEntity<>(insightsData, HttpStatus.OK);
    }

    /**
     * Retrieves data formatted for a Sankey diagram visualization.
     * <p>
     * Endpoint: GET /api/insights/stats/transactions/sankey
     *
     * @param fy     The Fiscal Year.
     * @param period The specific month name.
     * @param type   The entity type: soggetti, rapporti, datiContabili, or collegamenti.
     * @return A {@link InsightsTransactionSankeyBreakdownDTO} containing nodes and links for the diagram.
     */
    @GetMapping("/transactions/sankey")
    @Operation(summary = "Get Insights sankey chart", description = "Request to get Insights sankey chart by given FY and Period")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<InsightsTransactionSankeyBreakdownDTO> getTransactionsSankeyBreakdown(
            @RequestParam("fy") Integer fy,
            @RequestParam("period") String period,
            @RequestParam(value = "type", defaultValue = "soggetti") String type) throws NotFoundRecordException {
        InsightsTransactionSankeyBreakdownDTO insightsData = insightsService.getTransactionsSankeyBreakdown(fy, period, type);
        return new ResponseEntity<>(insightsData, HttpStatus.OK);
    }
}