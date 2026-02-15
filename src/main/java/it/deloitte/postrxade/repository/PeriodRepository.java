package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.ErrorType;
import it.deloitte.postrxade.entity.Period;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for ErrorType entity.
 */
@Repository
public interface PeriodRepository extends JpaRepository<Period, Long> {
    
    Optional<Period> findByName(String name);
    Optional<Period> findByOrder(Integer order);
}



