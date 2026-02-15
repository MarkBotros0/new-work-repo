package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import it.deloitte.postrxade.entity.SubmissionStatusGroup;
import lombok.*;

import java.util.List;

/**
 * DTO representing a Submission Status in the system.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SubmissionStatusDTO {

    private Long id;
    private String name;
    private String description;
    private Integer order;

    @JsonIgnore
    private List<SubmissionDTO> submissions;

    @JsonIgnore
    private List<LogDTO> beforeLogs;

    @JsonIgnore
    private List<LogDTO> afterLogs;

}
