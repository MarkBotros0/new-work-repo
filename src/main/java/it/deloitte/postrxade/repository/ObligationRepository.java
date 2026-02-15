package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.dto.FilterItemDTO;
import it.deloitte.postrxade.entity.Obligation;
import it.deloitte.postrxade.entity.Period;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA repository for the Obligation entity.
 */
@Repository
public interface ObligationRepository extends JpaRepository<Obligation, Long> {

    /**
     * Finds all distinct combinations of active periods and their associated fiscal years
     * from the Obligation table.
     *
     * This query uses a constructor expression to map the results directly into
     * a list of FilterItemDTO objects.
     *
     * Note: We cast fiscalYear (which is an Integer) to a String to match the
     * FilterItemDTO constructor (String period, String fiscalYear).
     *
     * @return A list of FilterItemDTOs representing unique period/year combinations.
     */
    @Query("SELECT DISTINCT new it.deloitte.postrxade.dto.FilterItemDTO(o.period.name, CAST(o.fiscalYear AS string)) " +
            "FROM Obligation o " +
            "WHERE o.period.isActive = true " +
            "ORDER BY o.fiscalYear DESC, o.period.order DESC")
    List<FilterItemDTO> findDistinctObligationFilters();

    Optional<Obligation> findByFiscalYearAndPeriod(Integer fiscalYear, Period period);
    Optional<Obligation> findByFiscalYearAndPeriod_Name(Integer fiscalYear, String periodName);
}