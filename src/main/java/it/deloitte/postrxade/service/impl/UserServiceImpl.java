package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.entity.Log;
import it.deloitte.postrxade.service.AuthorityService;
import it.deloitte.postrxade.service.UserService;
import it.deloitte.postrxade.entity.Authority;
import it.deloitte.postrxade.entity.User;
import it.deloitte.postrxade.dto.UserDTO;
import it.deloitte.postrxade.exception.AuthorityCodeNotValidException;
import it.deloitte.postrxade.exception.UserNotValidException;
import it.deloitte.postrxade.repository.AuthorityRepository;
import it.deloitte.postrxade.repository.UserRepository;
import it.deloitte.postrxade.utils.AuditLogger;
import ma.glasnost.orika.BoundMapperFacade;
import ma.glasnost.orika.MapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Implementation of {@link UserService} for managing {@link User}.
 */
@Service
@Transactional
public class UserServiceImpl implements UserService {

	private static final Logger LOGGER = LoggerFactory.getLogger(UserServiceImpl.class);
	private static final String LOGGER_MSG_BEGIN = "{}Inizio [hashcode={}]";
	private static final String LOGGER_MSG_BEGIN_STATIC = "{}Inizio";
	private static final String LOGGER_MSG_END = "{}Fine";

	private static final String USER_FIELD_SUB = "sub";
	private static final String USER_FIELD_NAME = "name";
	private static final String USER_FIELD_SURNAME = "surname";
	private static final String USER_FIELD_COMPANY = "company";
	private static final String USER_FIELD_DEPARTMENT = "department";

	private static final String USER_FIELD_EMAIL = "mail";
	private static final String USER_FIELD_GIVEN_NAME = "given_name";
	private static final String USER_FIELD_FAMILY_NAME = "family_name";
	private static final String USER_FIELD_PREFERRED_USERNAME = "preferred_username";
	private static final String USER_FIELD_EMAIL_ALT = "email";
	private static final String USR_DB = "{}userDb={}";


	private final UserRepository userRepository;

	private final AuthorityRepository authorityRepository;

	private final AuthorityService authorityService;

    private final AuditLogger appLogger;
	// private final CacheManager cacheManager;

	private MapperFactory mapperFactory;

	private BoundMapperFacade<User, UserDTO> userMapper;

	public UserServiceImpl(
            UserRepository userRepository,
            AuthorityRepository authorityRepository,
            AuthorityService authorityService, AuditLogger appLogger,
            // CacheManager cacheManager,
            @Qualifier("userServiceMapperFactory") MapperFactory mapperFactory
	) {
		this.userRepository = userRepository;
		this.authorityRepository = authorityRepository;
		this.authorityService = authorityService;
        this.appLogger = appLogger;
        // this.cacheManager = cacheManager;
		this.mapperFactory = mapperFactory;
	}

	@PostConstruct
	public void init() {
		String funcIdentifier = "[USIini] ";
		LOGGER.debug(LOGGER_MSG_BEGIN, funcIdentifier, this.hashCode());
		this.userMapper = mapperFactory.getMapperFacade(User.class, UserDTO.class);
		LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
	}

	// /**
	//  * Update basic information for the current user.
	//  *
	//  * @param firstName first name of user.
	//  * @param lastName  last name of user.
	//  * @param email     email id of user.
	//  * @param langKey   language key.
	//  * @param imageUrl  image URL of user.
	//  */
	// public void updateUser(String firstName, String lastName, String email, String langKey, String imageUrl) {
	// 	// SecurityUtils
	// 	// 	.getCurrentUserLogin()
	// 	// 	.flatMap(userRepository::findOneById)
	// 	// 	.ifPresent(
	// 	// 		user -> {
	// 	// 			// update user info

	// 	// 			// user.setFirstName(firstName);
	// 	// 			// user.setLastName(lastName);
	// 	// 			// if (email != null) {
	// 	// 			// 	user.setEmail(email.toLowerCase());
	// 	// 			// }
	// 	// 			// user.setLangKey(langKey);
	// 	// 			// user.setImageUrl(imageUrl);
	// 	// 			// this.clearUserCaches(user);
	// 	// 			LOGGER.debug("Changed Information for User: {}", user);
	// 	// 		}
	// 	// 	);
	// }

	@Transactional(readOnly = true)
	public Page<UserDTO> findAll(Pageable pageable) {
		String funcIdentifier = "[USIfa] ";
		LOGGER.debug(LOGGER_MSG_BEGIN, funcIdentifier, this.hashCode());
		LOGGER.debug("{}Request to get all UserDTO(s)", funcIdentifier);

		Page<UserDTO> result = userRepository
			.findAll(pageable)
			.map(userMapper::map);

		LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
		return result;
	}

	// @Transactional(readOnly = true)
	// public Page<AdminUserDTO> getAllManagedUsers(Pageable pageable) {
	// 	return userRepository.findAll(pageable).map(AdminUserDTO::new);
	// }

	// @Transactional(readOnly = true)
	// public Page<UserDTO> getAllPublicUsers(Pageable pageable) {
	// 	return userRepository.findAllByIdNotNullAndActivatedIsTrue(pageable).map(UserDTO::new);
	// }

	// @Transactional(readOnly = true)
	// public Optional<User> getUserWithAuthoritiesByLogin(String login) {
	// 	return userRepository.findOneWithAuthoritiesByLogin(login);
	// }

	/**
	 * Gets a list of all the authorities (only codes).
	 * @return a list of all the authorities.
	 */
	@Transactional(readOnly = true)
	public List<String> getAuthorities() {
		String funcIdentifier = "[USIGAuth] ";
		LOGGER.debug(LOGGER_MSG_BEGIN, funcIdentifier, this.hashCode());
		LOGGER.debug("{}Request to get all UserDTO(s)", funcIdentifier);

		List<String> result = authorityRepository
			.findAll()
			.stream()
			.map(Authority::getId)
			.collect(Collectors.toList());

		LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
		return result;
	}

	/**
	 * Questa funzione viene chiamata dalla func getUserFromAuthentication. Si occupa di recuperare l'utente dal db,
	 * se esiste, e di aggiornare per quest'ultimo il set di authoirities ad esso associato
	 * @param details di tipo Map<String, Object>. Sono gli attributi contenuti nel principal dell'authToken ricevuto dal SSO
	 * @param userDTO uno degli attributi di cui sopra
	 * @param profileAuthorities le grantedAuthoirties contenute nel token
	 * @return
	 */
	private UserDTO syncUserWithIdP(Map<String, Object> details, UserDTO userDTO, Set<String> profileAuthorities) {
		String funcIdentifier = "[USIsyncUs-"+userDTO.getId()+"] ";
		LOGGER.debug(LOGGER_MSG_BEGIN, funcIdentifier, this.hashCode());
		LOGGER.debug("{} Syncing user with id: {}", funcIdentifier, userDTO.getId());

		// User result = null;
		Set<Authority> authorities = getAuthorities(profileAuthorities);

		Optional<User> existingUser = userRepository.findById(userDTO.getId());
		User userDb = null;

		if (existingUser.isPresent()) {
			LOGGER.debug("{}User presente su db", funcIdentifier);

			userDb = existingUser.get();
			userDb.setAuthorities(authorities);
			for (Authority authority: authorities) {
				authority.getUsers().add(userDb);
			}

			userDb.setCompany(userDTO.getCompany());
			userDb.setOffice(userDTO.getOffice());
			userDb.setFirstName(userDTO.getFirstName());
			userDb.setLastName(userDTO.getLastName());
			userDb.setLastLoggedIn(LocalDateTime.now());

			userDb.setEmail(userDTO.getEmail());

			LOGGER.debug("{}Call to [userRepository.save(userDb)]", funcIdentifier);
			userDb = userRepository.save(userDb);
			LOGGER.debug(USR_DB, funcIdentifier, userDb);
		}
		else {
			LOGGER.debug("{}User non presente su db", funcIdentifier);

			userDb = new User();

			userDb.setAuthorities(authorities);
			for (Authority authority: authorities) {
				authority.getUsers().add(userDb);
			}
			userDb.setId(userDTO.getId());
			userDb.setCompany(userDTO.getCompany());
			userDb.setOffice(userDTO.getOffice());
			userDb.setFirstName(userDTO.getFirstName());
			userDb.setLastName(userDTO.getLastName());
			userDb.setLastLoggedIn(LocalDateTime.now());

			userDb.setEmail(userDTO.getEmail());

			LOGGER.debug("{}Call to [userRepository.save(userDb)]", funcIdentifier);
			userDb = userRepository.save(userDb);
			LOGGER.debug(USR_DB, funcIdentifier, userDb);


		}

		LOGGER.debug(USR_DB, funcIdentifier, userDb);
		UserDTO userDto = userMapper.map(userDb);


        //log it in the db
        appLogger.save(Log.builder()
                .updater(userDb)
                .message(String.format(appLogger.LOGIN_MESSAGE, userDb.getFirstName(), userDb.getLastName()))
                .timestamp(Instant.now())
                .build());


		LOGGER.debug("{}userDto={}", funcIdentifier, userDto);

		// // save authorities in to sync user roles/groups between IdP and local database
		// Collection<String> dbAuthorities = getAuthorities();
		// LOGGER.debug("dbAuthorities={}", dbAuthorities);

		// Collection<String> userAuthorities = user
		// 	.getAuthorities()
		// 	.stream()
		// 	.map(Authority::getId)
		// 	.collect(Collectors.toList());

		// for (String authority : userAuthorities) {
		// 	if (!dbAuthorities.contains(authority)) {
		// 		LOGGER.debug("Saving authority '{}' in local database", authority);
		// 		Authority authorityToSave = new Authority();
		// 		authorityToSave.setName(authority);
		// 		authorityRepository.save(authorityToSave);
		// 	}
		// }

		// // save account in to sync users between IdP and local database
		// Optional<User> existingUser = userRepository.findOneByLogin(user.getLogin());
		// if (existingUser.isPresent()) {
		// 	// if IdP sends last updated information, use it to determine if an update should happen
		// 	if (details.get("updated_at") != null) {
		// 		Instant dbModifiedDate = existingUser.get().getLastModifiedDate();
		// 		Instant idpModifiedDate = (Instant) details.get("updated_at");
		// 		if (idpModifiedDate.isAfter(dbModifiedDate)) {
		// 			LOGGER.debug("Updating user '{}' in local database", user.getLogin());
		// 			updateUser(user.getFirstName(), user.getLastName(), user.getEmail(), user.getLangKey(), user.getImageUrl());
		// 		}
		// 		// no last updated info, blindly update
		// 	}
		// 	else {
		// 		LOGGER.debug("Updating user '{}' in local database", user.getLogin());
		// 		updateUser(user.getFirstName(), user.getLastName(), user.getEmail(), user.getLangKey(), user.getImageUrl());
		// 	}
		// }
		// else {
		// 	LOGGER.debug("Saving user '{}' in local database", user.getLogin());
		// 	userRepository.save(user);
		// 	this.clearUserCaches(user);
		// }

		LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
		return userDto;
	}

	/**
	 * Returns the user from an OAuth 2.0 login or resource server with JWT.
	 * Synchronizes the user in the local repository.
	 *
	 * @param authToken the authentication token.
	 * @return the user from the authentication.
	 * @throws UserNotValidException
	 */
	// @Transactional
	public UserDTO getUserFromAuthentication(AbstractAuthenticationToken authToken) throws UserNotValidException {
		String funcIdentifier = "[GUsFAuth] ";
		LOGGER.debug(LOGGER_MSG_BEGIN, funcIdentifier, this.hashCode());
		LOGGER.debug("{} Getting user from auth token with name: {} ", funcIdentifier, authToken.getName());
		Map<String, Object> attributes;
		if (authToken instanceof OAuth2AuthenticationToken) {
			LOGGER.debug("{}Object authToken is instance of OAuth2AuthenticationToken", funcIdentifier);
			LOGGER.debug("{}Object authToken is this one: {}", funcIdentifier, authToken.toString());
			attributes = ((OAuth2AuthenticationToken) authToken).getPrincipal().getAttributes();

			// OAuth2User oauthUser = ((OAuth2AuthenticationToken) authToken).getPrincipal();
			// String oauthUserClass = oauthUser.getClass().getName();
			// LOGGER.debug("oauthUserClass={}", oauthUserClass);
		}
		else if (authToken instanceof JwtAuthenticationToken) {
			LOGGER.debug("{}Object authToken is NOT instance of OAuth2AuthenticationToken", funcIdentifier);
			LOGGER.debug("{}Object authToken is instance of JwtAuthenticationToken", funcIdentifier);
			LOGGER.debug("{}Object authToken is this one: {}", funcIdentifier, authToken.toString());
			attributes = ((JwtAuthenticationToken) authToken).getTokenAttributes();
		}
		else {
			throw new IllegalArgumentException("AuthenticationToken is not OAuth2 or JWT!");
		}

		LOGGER.debug("{}Log attributes key-value", funcIdentifier);
		for (Map.Entry<String, Object> entry : attributes.entrySet()) {
			LOGGER.debug("{}{}={}", funcIdentifier, entry.getKey(), entry.getValue());
		}

		UserDTO user = getUser(attributes);

		// user.setAuthorities(
		// 	authToken
		// 		.getAuthorities()
		// 		.stream()
		// 		.map(GrantedAuthority::getAuthority)
		// 		.map(
		// 			authority -> {
		// 				Authority auth = new Authority();
		// 				auth.setName(authority);
		// 				return auth;
		// 			}
		// 		)
		// 		.collect(Collectors.toSet())
		// );

		Set<String> authorities = authToken
			.getAuthorities()
			.stream()
			.map(GrantedAuthority::getAuthority)
			.map(
				grantedAuthority -> grantedAuthority
			)
			.collect(Collectors.toSet());

		String authoritiesString = Arrays.toString(authorities.toArray());
		LOGGER.debug("{}authorities={}", funcIdentifier, authoritiesString);

		// User userDb = syncUserWithIdP(attributes, user, authorities);
		// LOGGER.debug(USR_DB, userDb);
		// UserDTO userDto = userMapper.map(userDb);
		// LOGGER.debug("userDto={}", userDto);

		UserDTO userDto = syncUserWithIdP(attributes, user, authorities);
		LOGGER.debug("{}userDto={}",  funcIdentifier, userDto);

		LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
		return userDto;
	}

	private static UserDTO getUser(Map<String, Object> details) throws UserNotValidException {
		String funcIdentifier = "[USIGUs] ";
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC, funcIdentifier);
		UserDTO user = new UserDTO();

		// Boolean activated = Boolean.TRUE;

		if (details.get(USER_FIELD_SUB) instanceof String) {
			user.setId((String) details.get(USER_FIELD_SUB));
		}
		else {
			String errorPayload = "User attribute '" + USER_FIELD_SUB + "' could not be found.";
			LOGGER.warn(errorPayload);
			throw new UserNotValidException(errorPayload);
		}

		// Gestione nome e cognome: priorità a given_name/family_name (standard OIDC), 
		// poi a name/surname, infine parsing di name se contiene "Cognome, Nome" (Microsoft Entra)
		// Supporta entrambi i sistemi: Microsoft Entra (name="Cognome, Nome") e Nexi (name="Nome", surname="Cognome")
		String firstName = null;
		String lastName = null;

		// Prima prova con given_name e family_name (standard OIDC)
		if (details.get(USER_FIELD_GIVEN_NAME) instanceof String) {
			firstName = (String) details.get(USER_FIELD_GIVEN_NAME);
			LOGGER.debug("{}firstName from given_name={}", funcIdentifier, firstName);
		}
		if (details.get(USER_FIELD_FAMILY_NAME) instanceof String) {
			lastName = (String) details.get(USER_FIELD_FAMILY_NAME);
			LOGGER.debug("{}lastName from family_name={}", funcIdentifier, lastName);
		}

		// Se firstName non trovato, prova con name
		if (firstName == null && details.get(USER_FIELD_NAME) instanceof String) {
			String name = (String) details.get(USER_FIELD_NAME);
			LOGGER.debug("{}name={}", funcIdentifier, name);
			
			// Se name contiene una virgola, potrebbe essere formato "Cognome, Nome" (Microsoft Entra)
			if (name.contains(",")) {
				String[] parts = name.split(",", 2);
				if (parts.length == 2) {
					// Se lastName non è già stato impostato da family_name, usa il valore parsato
					if (lastName == null) {
						lastName = parts[0].trim();
					}
					firstName = parts[1].trim();
					LOGGER.debug("{}Parsed name (Microsoft Entra format): lastName={}, firstName={}", funcIdentifier, lastName, firstName);
				} else {
					firstName = name.trim();
				}
			} else {
				// name non contiene virgola, è probabilmente solo il nome (sistema Nexi)
				firstName = name.trim();
				LOGGER.debug("{}name used as firstName (Nexi format)={}", funcIdentifier, firstName);
			}
		}

		// Se lastName non trovato, prova con surname (sistema Nexi)
		if (lastName == null && details.get(USER_FIELD_SURNAME) instanceof String) {
			lastName = (String) details.get(USER_FIELD_SURNAME);
			LOGGER.debug("{}lastName from surname={}", funcIdentifier, lastName);
		}

		// Imposta i valori (default a "-" se non trovati)
		user.setFirstName(firstName != null ? firstName : "-");
		user.setLastName(lastName != null ? lastName : "-");

		// Gestione company
		if (details.get(USER_FIELD_COMPANY) instanceof String) {
			user.setCompany((String) details.get(USER_FIELD_COMPANY));
		}

		// Gestione department/office
		if (details.get(USER_FIELD_DEPARTMENT) instanceof String) {
			user.setOffice((String) details.get(USER_FIELD_DEPARTMENT));
		}

		// Gestione email: prova prima con "mail", poi con "preferred_username" (Microsoft Entra), infine con "email"
		String email = null;
		if (details.get(USER_FIELD_EMAIL) instanceof String) {
			email = (String) details.get(USER_FIELD_EMAIL);
			LOGGER.debug("{}email from mail={}", funcIdentifier, email);
		}
		if (email == null && details.get(USER_FIELD_PREFERRED_USERNAME) instanceof String) {
			email = (String) details.get(USER_FIELD_PREFERRED_USERNAME);
			LOGGER.debug("{}email from preferred_username={}", funcIdentifier, email);
		}
		if (email == null && details.get(USER_FIELD_EMAIL_ALT) instanceof String) {
			email = (String) details.get(USER_FIELD_EMAIL_ALT);
			LOGGER.debug("{}email from email={}", funcIdentifier, email);
		}
		if (email != null) {
			user.setEmail(email.toLowerCase());
		}
		LOGGER.debug("{}final email={}", funcIdentifier, user.getEmail());
		//todo devi capire se mettere l-eccezione. in ottica delle mail, non ricevere la mail nel token puo essere un
		// problema quindi magari meglio metterlo il token

		// // handle resource server JWT, where sub claim is email and uid is ID
		// if (details.get("uid") != null) {
		// 	LOGGER.debug("User uid NOT NULL > Set UID from uid");
		// 	LOGGER.debug("uid={}", details.get("uid"));
		// 	user.setId((String) details.get("uid"));
		// 	// user.setLogin((String) details.get("sub"));
		// }
		// else {
		// 	LOGGER.debug("User uid NULL > Set UID from sub");
		// 	user.setId((String) details.get("sub"));
		// }

		// if (details.get("preferred_username") != null) {
		// 	user.setLogin(((String) details.get("preferred_username")).toLowerCase());
		// }
		// else if (user.getLogin() == null) {
		// 	user.setLogin(user.getId());
		// }

		// if (details.get("given_name") != null) {
		// 	user.setFirstName((String) details.get("given_name"));
		// }

		// if (details.get("family_name") != null) {
		// 	user.setLastName((String) details.get("family_name"));
		// }

		// if (details.get("email_verified") != null) {
		// 	activated = (Boolean) details.get("email_verified");
		// }

		// if (details.get("email") != null) {
		// 	user.setEmail(((String) details.get("email")).toLowerCase());
		// }
		// else {
		// 	user.setEmail((String) details.get("sub"));
		// }

		// if (details.get("langKey") != null) {
		// 	user.setLangKey((String) details.get("langKey"));
		// }
		// else if (details.get("locale") != null) {
		// 	// trim off country code if it exists
		// 	String locale = (String) details.get("locale");
		// 	if (locale.contains("_")) {
		// 		locale = locale.substring(0, locale.indexOf('_'));
		// 	}
		// 	else if (locale.contains("-")) {
		// 		locale = locale.substring(0, locale.indexOf('-'));
		// 	}
		// 	user.setLangKey(locale.toLowerCase());
		// }
		// else {
		// 	// set langKey to default if not specified by IdP
		// 	user.setLangKey(Constants.DEFAULT_LANGUAGE);
		// }

		// if (details.get("picture") != null) {
		// 	user.setImageUrl((String) details.get("picture"));
		// }

		// user.setActivated(activated);

		LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
		return user;
	}

	/**
	 * Converte UserDTO in AccountDTO per evitare riferimenti circolari
	 * @param userDTO il UserDTO da convertire
	 * @return AccountDTO senza riferimenti circolari
	 */


	/**
	 * funzione chiamata dalla syncUserWithIdP. Per le granted authoirities contenute nel token cerca di recuperarle
	 * dal db
	 * @param profileAuthorities le granted authoirities contenute nell'Auth token
	 * @return
	 */
	private Set<Authority> getAuthorities(Set<String> profileAuthorities) {
		String funcIdentifier = "[USIGAuth] ";
		LOGGER.debug(LOGGER_MSG_BEGIN, funcIdentifier, this.hashCode());

		Set<Authority> authorities = new HashSet<>();

		for (String profileAuthority : profileAuthorities) {
			LOGGER.debug("{}profileAuthority={}", funcIdentifier, profileAuthority);

			try {
				Authority authority = authorityService.getAuthorityFromCode(profileAuthority);
				authorities.add(authority);
			}
			catch(AuthorityCodeNotValidException ex) {
				String errorPayload = "Authority code non valido > Authority non riconosciuta [code='" + profileAuthority + "']";
				LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"));
			}
		}
		LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
		return authorities;
	}

	// private void clearUserCaches(User user) {
	// 	Objects.requireNonNull(cacheManager.getCache(UserRepository.USERS_BY_LOGIN_CACHE)).evict(user.getLogin());
	// 	if (user.getEmail() != null) {
	// 		Objects.requireNonNull(cacheManager.getCache(UserRepository.USERS_BY_EMAIL_CACHE)).evict(user.getEmail());
	// 	}
	// }

	public UserDTO getCurrentUser() throws UserNotValidException {
		String funcIdentifier = "[USIGCurrUs] ";
		LOGGER.debug(LOGGER_MSG_BEGIN, funcIdentifier, this.hashCode());

		SecurityContext securityContext = SecurityContextHolder.getContext();
		Authentication authentication = securityContext.getAuthentication();

		Map<String, Object> attributes = null;
		if (authentication instanceof OAuth2AuthenticationToken) {
			LOGGER.debug("{}Object authentication is instance of OAuth2AuthenticationToken", funcIdentifier);
			attributes = ((OAuth2AuthenticationToken) authentication).getPrincipal().getAttributes();
		}
		else if (authentication instanceof JwtAuthenticationToken) {
			LOGGER.debug("{}Object authentication is NOT instance of OAuth2AuthenticationToken", funcIdentifier);
			LOGGER.debug("{}Object authentication is instance of JwtAuthenticationToken", funcIdentifier);
			attributes = ((JwtAuthenticationToken) authentication).getTokenAttributes();
		}
		else {
			throw new IllegalArgumentException("AuthenticationToken is not OAuth2 or JWT!");
		}

		UserDTO userDTO = getUser(attributes);

		Optional<User> existingUser = userRepository.findById(userDTO.getId());
		UserDTO result = null;

		if (existingUser.isPresent()) {
			LOGGER.debug("{}User presente su db", funcIdentifier);
			User userDb = existingUser.get();
			LOGGER.debug(USR_DB, funcIdentifier, userDb);
			result = userMapper.map(userDb);
			LOGGER.debug("{}result={}", funcIdentifier, result);
		}
		else {
			String errorPayload = "{}Current user could not be found";
			LOGGER.warn(errorPayload, funcIdentifier);
			errorPayload = "Current user could not be found";
			throw new UserNotValidException(errorPayload);
		}

		LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
		return result;
	}

}
