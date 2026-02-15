package it.deloitte.postrxade.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DTO representing an Error Type in the system.
 */
@Setter
@Getter
public class ErrorTypeDTO {

    // Getters and Setters
    private Long id;
    private String name;
    private String description;
    private Integer severityLevel;
    private Boolean isActive;

    // Constructors
    public ErrorTypeDTO() {}

    public ErrorTypeDTO(Long id, String name, String description, Integer severityLevel, Boolean isActive) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.severityLevel = severityLevel;
        this.isActive = isActive;
    }

    @Override
    public String toString() {
        return "ErrorTypeDTO{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", severityLevel=" + severityLevel +
                ", isActive=" + isActive +
                '}';
    }
}

