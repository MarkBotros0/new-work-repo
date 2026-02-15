package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.Authority;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AuthorityRepository extends JpaRepository<Authority, String> {

    Optional<Authority> findById(String id);

    boolean existsById(String id);
}

