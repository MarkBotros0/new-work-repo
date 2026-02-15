package it.deloitte.postrxade.dto;

import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * Represents the top-level JSON object for the filter options API response.
 * It contains a list of FilterItemDTO objects.
 */
@Data
@ToString
public class FilterOptionsDTO {

    private List<FilterItemDTO> filters;

    public FilterOptionsDTO(List<FilterItemDTO> filters) {
        this.filters = filters;
    }

}
