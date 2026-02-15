package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

/**
 * DTO representing the dashboard statistics to be sent to the frontend.
 * The @JsonProperty annotation ensures the JSON output keys match the snake_case format
 * expected by the frontend.
 */
@Data
@ToString
public class DashboardStatsDTO {

    @JsonProperty("failed")
    private Integer failed;

    @JsonProperty("processing")
    private Integer processing;

    @JsonProperty("success")
    private Integer success;

    @JsonProperty("total_transactions")
    private Long totalTransactions;

    @JsonProperty("total_previous_period_transactions")
    private Long totalPreviousTransactions;

    @JsonProperty("total_reportable_transactions")
    private Long totalReportableTransactions;

    @JsonProperty("total_previous_reportable_transactions")
    private Long totalPreviousReportableTransactions;

    @JsonProperty("error")
    private Long error;

    @JsonProperty("warning")
    private Long warning;

    @JsonProperty("obligation_registered_abandoned")
    private Integer obligationRegisteredAbandoned;

    @JsonProperty("obligation_total")
    private Integer obligationTotal;

    @JsonProperty("obligation_approved")
    private Integer obligationApproved;

    @JsonProperty("obligation_completed")
    private Integer obligationCompleted;

}