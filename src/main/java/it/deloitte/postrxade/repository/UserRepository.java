package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface UserRepository extends JpaRepository<User, String> {
	Optional<User> findById(String id);

	@Query("""
            select u from User u
            join fetch u.authorities
            where u.id = :id
            """)
	Optional<User> findByIdWithAuthorities(String id);

}
