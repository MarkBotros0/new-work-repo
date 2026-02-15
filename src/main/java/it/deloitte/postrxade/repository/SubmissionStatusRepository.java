package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.SubmissionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SubmissionStatusRepository extends JpaRepository<SubmissionStatus, Long> {
    Optional<SubmissionStatus> findOneByOrder(Integer order);

}
