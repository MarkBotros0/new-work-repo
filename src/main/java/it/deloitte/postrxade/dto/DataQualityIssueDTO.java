package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

import java.util.List;

/**
 * Data Transfer Object for a specific Data Quality (DQ) issue.
 * <p>
 * This record holds details about a specific validation failure,
 * including the column, descriptions, severity (error_level),
 * occurrence count, and a list of example transaction IDs.
 */
@Builder
public record DataQualityIssueDTO(
        @JsonProperty("error_level")
        String errorLevel,
        @JsonProperty("error_code")
        String errorCode,
        @JsonProperty("error_name")
        String errorName,
        @JsonProperty("description")
        String description,
        @JsonProperty("errors_count")
        int errorsCount,
        @JsonProperty("examples")
        List<String> examples
) {}