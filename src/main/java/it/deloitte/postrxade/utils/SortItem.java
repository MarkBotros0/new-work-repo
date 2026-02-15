package it.deloitte.postrxade.utils;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.domain.Sort;

import java.io.Serializable;

/**
 * DTO representing a single sorting criterion.
 * <p>
 * Used in search DTOs (like {@code AuditLogsSearchDTO}) to define how results should be ordered.
 */
@Getter
@Setter // Added Setter to ensure JSON deserialization works correctly
@Data   // Added Data for toString/equals/hashCode
public class SortItem implements Serializable {

    /**
     * The name of the field to sort by (must match the Entity field name).
     */
    @Schema(example = "timestamp", description = "The field name to sort by")
    private String field;

    /**
     * The direction of the sort (ASC or DESC).
     */
    @Schema(description = "The direction of the sort")
    private Sort.Direction direction;
}
