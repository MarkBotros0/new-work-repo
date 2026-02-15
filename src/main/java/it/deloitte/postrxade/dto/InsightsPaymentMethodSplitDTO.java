package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InsightsPaymentMethodSplitDTO {
    // Using 'double' for percentage, but 'int' or 'BigDecimal'
    // could also be used depending on your needs.
    private double percentage;

    @JsonProperty("transaction_type")
    private String transactionType;

    @JsonProperty("__typename")
    private String __typename;

}
