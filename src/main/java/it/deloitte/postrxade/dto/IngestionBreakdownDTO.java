package it.deloitte.postrxade.dto;

import lombok.Data;
import lombok.ToString;

/**
 * DTO representing the breakdown of accepted and rejected transactions for a specific quarter
 * within an ingestion batch.
 */
@Data
@ToString
public class IngestionBreakdownDTO {

    private String title;
    private String accepted;
    private String rejected;

    public IngestionBreakdownDTO(String title, String accepted, String rejected) {
        this.title = title;
        this.accepted = accepted;
        this.rejected = rejected;
    }

}
