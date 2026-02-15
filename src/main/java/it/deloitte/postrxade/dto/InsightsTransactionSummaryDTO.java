package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

@Data
public class InsightsTransactionSummaryDTO {

    @JsonProperty("total_transactions")
    private Long totalTransactions;

    @JsonProperty("total_previous_period_transactions")
    private Long totalPreviousTransactions;

    @JsonProperty("total_reportable_transactions")
    private Long totalReportableTransactions;

    @JsonProperty("total_previous_reportable_transactions")
    private Long totalPreviousReportableTransactions;


}