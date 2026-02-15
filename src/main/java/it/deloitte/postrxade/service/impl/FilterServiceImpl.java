package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.dto.FilterItemDTO;
import it.deloitte.postrxade.dto.FilterOptionsDTO;
import it.deloitte.postrxade.repository.ObligationRepository;
import it.deloitte.postrxade.service.FilterService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Implementation of the FilterService.
 * <p>
 * This service is responsible for supplying the dynamic options used in the UI filters.
 * It queries the database to find distinct combinations of Fiscal Years and Periods
 * that actally contain data, preventing users from selecting empty ranges.
 */
@Service
public class FilterServiceImpl implements FilterService {

    private final ObligationRepository obligationRepository;

    /**
     * Constructor-based dependency injection.
     *
     * @param obligationRepository The repository to query for distinct filter values.
     */
    public FilterServiceImpl(ObligationRepository obligationRepository) {
        this.obligationRepository = obligationRepository;
    }

    /**
     * Retrieves the distinct filter options available in the system.
     * <p>
     * Logic:
     * Delegates to the repository to perform a distinct query on Obligations,
     * returning a list of available Fiscal Years and Periods.
     *
     * @return {@link FilterOptionsDTO} wrapping the list of {@link FilterItemDTO}.
     */
    @Override
    @Transactional(readOnly = true)
    public FilterOptionsDTO getFilterOptions() {

        List<FilterItemDTO> filterItems = obligationRepository.findDistinctObligationFilters();

        return new FilterOptionsDTO(filterItems);
    }
}
