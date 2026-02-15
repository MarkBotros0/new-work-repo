package it.deloitte.postrxade.dto;

import it.deloitte.postrxade.entity.SubmissionStatus;
import lombok.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * DTO representing a Submission.
 * Corresponds to the 'Submission' interface in FE.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SubmissionCustomDTO {

    private Long id;
    private SubmissionStatusDTO currentSubmissionStatus;
    private Integer ingestionsCount;
    private List<ValidationDTO> validations;
    private LocalDate deadlineDate;
    private boolean isManual;
    private LocalDateTime approvedAt;
    private LocalDateTime cancelledAt;
    private SubmissionStatusDTO lastSubmissionStatus;
}

