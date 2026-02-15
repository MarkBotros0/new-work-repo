package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.SubmissionStatusGroup;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for Merchant entity.
 */
@Repository
public interface SubmissionStatusGroupRepository extends JpaRepository<SubmissionStatusGroup, Long> {

    /**
     * Uses the NamedEntityGraph to fetch the group and its statuses.
     * Spring Data will see @EntityGraph and use it.
     */
    @EntityGraph(value = "SubmissionStatusGroup.withStatuses")
    Optional<SubmissionStatusGroup> findById(Long id); // You can just override findById

    @EntityGraph(value = "SubmissionStatusGroup.withStatuses")
    List<SubmissionStatusGroup> findAll();
}