package it.deloitte.postrxade.dto;

import lombok.*;

import java.util.List;

/**
 * DTO representing a single ingestion batch record, as shown in the Ingestion Page table.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class IngestionBatchDTO {

    private String batchId;
    private String uploadTime;
    private String month;
    private List<IngestionTransactionsDTO> transactions;
    private List<IngestionBreakdownDTO> breakdown;
    private String status;
    private String error;
    private String message;


}
