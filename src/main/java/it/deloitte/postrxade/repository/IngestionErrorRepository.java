package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.IngestionError;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Repository for IngestionError entity.
 */
@Repository
public interface IngestionErrorRepository extends JpaRepository<IngestionError, Long> {
    
    // Basic findById is inherited from JpaRepository
}



