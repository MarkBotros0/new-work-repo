package it.deloitte.postrxade.service;

import it.deloitte.postrxade.dto.FilterOptionsDTO;

/**
 * Service interface for business logic related to fetching filter data.
 * <p>
 * This service is responsible for providing the dynamic options (Fiscal Years, Periods)
 * required to populate UI filters.
 */
public interface FilterService {

    /**
     * Retrieves the available options for filtering, such as periods and years.
     *
     * @return a {@link FilterOptionsDTO} containing the list of filter options.
     */
    FilterOptionsDTO getFilterOptions();
}
