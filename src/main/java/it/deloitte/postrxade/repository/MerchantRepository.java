package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.Merchant;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for Merchant entity.
 */
@Repository
public interface MerchantRepository extends JpaRepository<Merchant, Long>, MerchantRepositoryCustom {
    long countByIngestionId(Long ingestion);

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM Merchant m
            WHERE m.submission.id = :submissionId
            """)
    int deleteBySubmissionId(@Param("submissionId") Long submissionId);
}



