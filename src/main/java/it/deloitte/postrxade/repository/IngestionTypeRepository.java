package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.IngestionType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

/**
 * Repository for IngestionType entity.
 */
@Repository
public interface IngestionTypeRepository extends JpaRepository<IngestionType, Long> {

    Optional<IngestionType> findByNameIgnoreCase(String name);
}





