package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

@Data
public class InsightsTransactionBreakdownDTO {

    @JsonProperty("total_transactions")
    private Long totalTransactions;

    @JsonProperty("total_reportable_transactions")
    private Long totalReportableTransactions;

    @JsonProperty("fiscal_year")
    private int fiscalYear;

    @JsonProperty("period")
    private String period;

}
