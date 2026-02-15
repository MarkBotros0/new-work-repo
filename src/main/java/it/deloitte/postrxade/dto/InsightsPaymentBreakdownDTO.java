package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InsightsPaymentBreakdownDTO {
    private int day;

    private long count;

    @JsonProperty("transaction_date")
    private LocalDate transactionDate;

    @JsonProperty("__typename")
    private String __typename;
}
