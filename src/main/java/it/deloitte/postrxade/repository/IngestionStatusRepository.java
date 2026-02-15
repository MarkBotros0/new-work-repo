package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.IngestionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for IngestionStatus entity.
 */
@Repository
public interface IngestionStatusRepository extends JpaRepository<IngestionStatus, Long> {
    Optional<IngestionStatus> findOneByName(String name);
}





