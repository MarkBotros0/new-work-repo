package it.deloitte.postrxade.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

/**
 * DTO representing a Period in the system.
 */
@Setter
@Getter
@NoArgsConstructor
public class PeriodDTO {

    private Long id;
    private String name;
    private String description;
    private Integer order;
    private Boolean isActive;
    private List<ObligationDTO> obligations;

    public PeriodDTO(Long id, String name, String description, Integer order, Boolean isActive) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.order = order;
        this.isActive = isActive;
    }

    @Override
    public String toString() {
        return "PeriodDTO{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", order=" + order +
                ", isActive=" + isActive +
                '}';
    }
}
