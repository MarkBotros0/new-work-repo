package it.deloitte.postrxade.service;

import it.deloitte.postrxade.entity.User;
import it.deloitte.postrxade.dto.UserDTO;

import it.deloitte.postrxade.exception.UserNotValidException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.authentication.AbstractAuthenticationToken;

import java.util.List;

/**
 * Service interface for managing {@link User}.
 */
public interface UserService {

	Page<UserDTO> findAll(Pageable pageable);

	/**
	 * Gets a list of all the authorities (only codes).
	 * @return a list of all the authorities.
	 */
    List<String> getAuthorities();

	/**
	 * Returns the user from an OAuth 2.0 login or resource server with JWT.
	 * Synchronizes the user in the local repository.
	 *
	 * @param authToken the authentication token.
	 * @return the user from the authentication.
 	 * @throws UserNotValidException if the User detected by the system does not exist in the database
	*/
    UserDTO getUserFromAuthentication(AbstractAuthenticationToken authToken) throws UserNotValidException;

	UserDTO getCurrentUser() throws UserNotValidException;

	/**
	 * Converte UserDTO in AccountDTO per evitare riferimenti circolari
	 * @param userDTO il UserDTO da convertire
	 * @return AccountDTO senza riferimenti circolari
	 */

}
