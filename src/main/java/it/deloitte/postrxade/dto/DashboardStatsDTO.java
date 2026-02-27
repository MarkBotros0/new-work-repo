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

    @JsonProperty("error")
    private Long error;

    @JsonProperty("warning")
    private Long warning;

    @JsonProperty("obligationRejectedAbandoned")
    private Integer obligationRejectedAbandoned;

    @JsonProperty("obligationTotal")
    private Integer obligationTotal;

    @JsonProperty("obligationApproved")
    private Integer obligationApproved;

    @JsonProperty("obligationCompleted")
    private Integer obligationCompleted;
}