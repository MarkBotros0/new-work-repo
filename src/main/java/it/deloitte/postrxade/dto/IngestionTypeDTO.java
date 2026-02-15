package it.deloitte.postrxade.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * DTO representing an Ingestion Type in the system.
 */
@Setter
@Getter
@NoArgsConstructor
public class IngestionTypeDTO {

    // Getters and Setters
    private Long id;
    private String name;
    private String description;
    private Integer order;
    private Boolean isActive;
    private List<IngestionDTO> ingestions;

    public IngestionTypeDTO(Long id, String name, String description, Integer order, Boolean isActive) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.order = order;
        this.isActive = isActive;
    }

    @Override
    public String toString() {
        return "IngestionTypeDTO{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", order=" + order +
                ", isActive=" + isActive +
                '}';
    }
}
