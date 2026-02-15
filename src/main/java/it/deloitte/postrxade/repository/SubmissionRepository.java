package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.entity.SubmissionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SubmissionRepository extends JpaRepository<Submission, String> {

	Optional<Submission> findOneById(Long id);

    List<Submission> findByObligationId(Long obligationId);

    @Modifying
    @Query("UPDATE Submission s SET s.currentSubmissionStatus = :status WHERE s.id = :id")
    void updateStatus(@Param("id") Long id, @Param("status") SubmissionStatus status);
}
