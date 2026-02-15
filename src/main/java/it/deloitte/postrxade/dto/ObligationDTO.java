package it.deloitte.postrxade.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * DTO representing an Obligation in the system.
 */
@Setter
@Getter
@NoArgsConstructor
public class ObligationDTO {

    // Getters and Setters
    private Long id;
    private PeriodDTO period;
    private Integer fiscalYear;
    private List<SubmissionDTO> submissions;

    public ObligationDTO(Long id, PeriodDTO period, Integer fiscalYear) {
        this.id = id;
        this.period = period;
        this.fiscalYear = fiscalYear;
    }

    @Override
    public String toString() {
        return "ObligationDTO{" +
                "id=" + id +
                ", period=" + (period != null ? period.getName() : null) +
                ", fiscalYear=" + fiscalYear +
                '}';
    }
}
