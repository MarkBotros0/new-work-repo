package it.deloitte.postrxade.dto;

import lombok.*;

/**
 * DTO representing an Error Record in the system.
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class ErrorRecordDTO {

    private Long id;
    private IngestionDTO ingestion;
    private ErrorTypeDTO errorType;
    private String errorMessage;
    private String rawRow;


}


