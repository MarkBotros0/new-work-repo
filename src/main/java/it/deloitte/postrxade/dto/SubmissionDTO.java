package it.deloitte.postrxade.dto;

import lombok.*;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.List;

/**
 * DTO representing a Submission in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SubmissionDTO {

    private Long id;
    private SubmissionStatusDTO currentSubmissionStatus;
    private UserDTO lastUpdateBy;
    private ObligationDTO obligation;
    private LocalDateTime lastUpdatedAt;
    private LocalDateTime approvedAt;
    private LocalDate deadlineDate;
    private Boolean isManual;
    private LocalDateTime cancelledAt;
    private SubmissionStatusDTO lastSubmissionStatus;
    private List<IngestionDTO> ingestions;
    private List<OutputDTO> outputs;
    private List<LogDTO> logs;


}
