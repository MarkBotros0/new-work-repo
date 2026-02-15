package it.deloitte.postrxade.service;

import it.deloitte.postrxade.entity.Authority;
import it.deloitte.postrxade.exception.ActionNotPermittedException;
import it.deloitte.postrxade.exception.AuthorityCodeNotValidException;
import it.deloitte.postrxade.exception.UserNotValidException;

import java.util.Set;

/**
 * Service Interface for managing {@link it.deloitte.postrxade.entity.Authority}.
 */
public interface AuthorityService {

	Authority getAuthorityFromCode(String code) throws AuthorityCodeNotValidException;

	void checkUserAuthority(String userId, Set<String> authorizedCodes) throws UserNotValidException, ActionNotPermittedException;
}
