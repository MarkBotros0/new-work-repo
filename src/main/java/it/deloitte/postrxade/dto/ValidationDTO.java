package it.deloitte.postrxade.dto;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * DTO representing validation information (error or warning).
 * Corresponds to the 'Validation' interface in TypeScript.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidationDTO {
    /**
     * The name of the validation type, e.g., "error" or "warning".
     */
    private String name;

    /**
     * The count for this validation type.
     */
    private long count;
}
