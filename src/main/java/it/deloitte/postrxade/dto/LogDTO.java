package it.deloitte.postrxade.dto;

import it.deloitte.postrxade.utils.SortItem;
import lombok.*;

import java.time.Instant;
import java.util.List;

/**
 * DTO representing a Log in the system.
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class LogDTO {

    private Long id;
    private SubmissionDTO submission;
    private UserDTO updater;
    private SubmissionStatusDTO beforeSubmissionStatus;
    private SubmissionStatusDTO afterSubmissionStatus;
    private String message;
    private Instant timestamp;

}
