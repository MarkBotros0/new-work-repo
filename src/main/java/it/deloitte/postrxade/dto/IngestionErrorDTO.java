package it.deloitte.postrxade.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * DTO representing an Ingestion Error in the system.
 */
@Setter
@Getter
@NoArgsConstructor
public class IngestionErrorDTO {

    private Long id;
    private String name;
    private String description;
    private Integer order;
    private Boolean isActive;
    private List<IngestionDTO> ingestions;


    @Override
    public String toString() {
        return "IngestionErrorDTO{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", order=" + order +
                ", isActive=" + isActive +
                '}';
    }
}

