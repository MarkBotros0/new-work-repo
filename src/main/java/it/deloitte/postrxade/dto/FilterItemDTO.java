package it.deloitte.postrxade.dto;

import lombok.Data;
import lombok.ToString;

/**
 * Represents a single filter item containing a period and a year.
 * This corresponds to an object within the "filters" array in the JSON response.
 */
@Data
@ToString
public class FilterItemDTO {

    private String period;
    private String fiscalYear;

    public FilterItemDTO(String period, String year) {
        this.period = period;
        this.fiscalYear = year;
    }

}
