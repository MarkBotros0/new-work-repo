package it.deloitte.postrxade.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.List;

/**
 * DTO representing an Ingestion in the system.
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class IngestionDTO {

    // Getters and Setters
    private Long id;
    private IngestionTypeDTO ingestionType;
    private IngestionStatusDTO ingestionStatus;
    private SubmissionDTO submission;
    private IngestionErrorDTO ingestionError;
    private LocalDateTime ingestedAt;
    private String fullPath;
    private List<TransactionDTO> transactions;



    @Override
    public String toString() {
        return "IngestionDTO{" +
                "id=" + id +
                ", ingestionType=" + (ingestionType != null ? ingestionType.getName() : null) +
                ", ingestionStatus=" + (ingestionStatus != null ? ingestionStatus.getName() : null) +
                ", submission=" + (submission != null ? submission.getId() : null) +
                ", ingestedAt=" + ingestedAt +
                ", ingestionError=" + (ingestionError != null ? ingestionError.getName() : null) +
                ", fullPath='" + fullPath + '\'' +
                '}';
    }
}
