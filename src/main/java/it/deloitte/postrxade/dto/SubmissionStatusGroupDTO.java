package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.List;

/**
 * DTO for transferring SubmissionStatusGroup data.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SubmissionStatusGroupDTO {

    private Long id;

    @NotBlank(message = "Name cannot be blank")
    private String name;

    private String description;

    @JsonIgnore
    @NotBlank(message = "Code cannot be blank")
    private String code;

    @NotNull(message = "Order cannot be null")
    private Integer order;

    @JsonIgnore
    @NotNull(message = "isActive flag cannot be null")
    private Boolean isActive;

    private List<SubmissionStatusDTO> submissionStatuses;
}