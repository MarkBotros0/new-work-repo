package it.deloitte.postrxade.dto;

import lombok.Data;
import lombok.ToString;

/**
 * DTO representing the breakdown of accepted and rejected transactions for a specific quarter
 * within an ingestion batch.
 */
@Data
public class IngestionTransactionsDTO {

    private String title;
    private String count;

    public IngestionTransactionsDTO(String title, String count) {
        this.title = title;
        this.count = count;
    }

}
