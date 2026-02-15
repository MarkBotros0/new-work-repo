package it.deloitte.postrxade.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SubmissionStatusChangeDTO {

    private Long submissionId;
    private Integer destinationStatusOrder;

}
