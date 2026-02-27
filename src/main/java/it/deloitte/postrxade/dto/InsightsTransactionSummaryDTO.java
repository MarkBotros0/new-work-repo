package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

@Data
public class InsightsTransactionSummaryDTO {
    @JsonProperty("soggettiReceived")
    private Long soggettiReceived;

    @JsonProperty("soggettiAccepted")
    private Long soggettiAccepted;

    @JsonProperty("rapportiReceived")
    private Long rapportiReceived;

    @JsonProperty("rapportiAccepted")
    private Long rapportiAccepted;

    @JsonProperty("collegamentiReceived")
    private Long collegamentiReceived;

    @JsonProperty("collegamentiAccepted")
    private Long collegamentiAccepted;

    @JsonProperty("datiContabiliReceived")
    private Long datiContabiliReceived;

    @JsonProperty("datiContabiliAccepted")
    private Long datiContabiliAccepted;

    @JsonProperty("previousSoggettiReceived")
    private Long previousSoggettiReceived;

    @JsonProperty("previousSoggettiAccepted")
    private Long previousSoggettiAccepted;

    @JsonProperty("previousRapportiReceived")
    private Long previousRapportiReceived;

    @JsonProperty("previousRapportiAccepted")
    private Long previousRapportiAccepted;

    @JsonProperty("previousCollegamentiReceived")
    private Long previousCollegamentiReceived;

    @JsonProperty("previousCollegamentiAccepted")
    private Long previousCollegamentiAccepted;

    @JsonProperty("previousDatiContabiliReceived")
    private Long previousDatiContabiliReceived;

    @JsonProperty("previousDatiContabiliAccepted")
    private Long previousDatiContabiliAccepted;
}