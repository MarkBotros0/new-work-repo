package it.deloitte.postrxade.dto;

import lombok.*;

/**
 * DTO representing an Output in the system.
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OutputDTO {

    private Long id;
    private SubmissionDTO submission;
    private String fullPath;
    private String extensionType;
    private String generatedAt;


}
