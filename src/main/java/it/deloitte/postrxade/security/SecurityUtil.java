package it.deloitte.postrxade.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for Spring Security.
 */
public final class SecurityUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(SecurityUtil.class);
	private static final String LOGGER_MSG_BEGIN_STATIC = "Inizio";
	private static final String LOGGER_MSG_END = "Fine";

	private static final String ROLES_DELIMETER = " ";
	private static final String ROLES_PREFIX = "ICS_";
	private static final String PREFFER_USERNAME = "preferred_username";

	private static String userRole = "SPR"; // Default value

	/**
	 * Sets the user role from configuration.
	 * This method should be called by a Spring configuration class to initialize the static field.
	 *
	 * @param role the user role to set
	 */
	public static void setUserRole(String role) {
		userRole = role;
		LOGGER.debug("userRole initialized to: {}", userRole);
	}

	// oauth

	private SecurityUtil() {}

	/**
	 * Get the login of the current user.
	 *
	 * @return the login of the current user.
	 */
	public static Optional<String> getCurrentUserLogin() {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);
		SecurityContext securityContext = SecurityContextHolder.getContext();
		Optional<String> result = Optional.ofNullable(extractPrincipal(securityContext.getAuthentication()));
		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	private static String extractPrincipal(Authentication authentication) {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);

		String result = null;
		if (authentication != null) {
			if (authentication.getPrincipal() instanceof UserDetails) {
				LOGGER.debug("authentication.getPrincipal() instanceof UserDetails");
				UserDetails springSecurityUser = (UserDetails) authentication.getPrincipal();
				result = springSecurityUser.getUsername();
			}
			else if (authentication instanceof JwtAuthenticationToken) {
				LOGGER.debug("authentication instanceof JwtAuthenticationToken");
				result =  (String) ((JwtAuthenticationToken) authentication).getToken().getClaims().get(PREFFER_USERNAME);
			}
			else if (authentication.getPrincipal() instanceof DefaultOidcUser) {
				LOGGER.debug("authentication.getPrincipal() instanceof DefaultOidcUser");
				Map<String, Object> attributes = ((DefaultOidcUser) authentication.getPrincipal()).getAttributes();
				if (attributes.containsKey(PREFFER_USERNAME)) {
					result =  (String) attributes.get(PREFFER_USERNAME);
				}
			}
			else if (authentication.getPrincipal() instanceof String) {
				LOGGER.debug("authentication.getPrincipal() instanceof String");
				result = (String) authentication.getPrincipal();
			}
		}

		LOGGER.debug("principal={}", result);
		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	/**
	 * Check if a user is authenticated.
	 *
	 * @return true if the user is authenticated, false otherwise.
	 */
	public static boolean isAuthenticated() {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);
		Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
		boolean result = authentication != null && getAuthorities(authentication).noneMatch(AuthoritiesConstants.ANONYMOUS::equals);
		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	/**
	 * Checks if the current user has a specific authority.
	 *
	 * @param authority the authority to check.
	 * @return true if the current user has the authority, false otherwise.
	 */
	public static boolean hasCurrentUserThisAuthority(String authority) {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);
		Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
		boolean result = authentication != null && getAuthorities(authentication).anyMatch(authority::equals);
		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	private static Stream<String> getAuthorities(Authentication authentication) {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);

		Collection<? extends GrantedAuthority> authorities = null;
		if (authentication instanceof JwtAuthenticationToken) {
			authorities = extractAuthorityFromClaims(((JwtAuthenticationToken) authentication).getToken().getClaims());
		}
		else {
			authorities = authentication.getAuthorities();
		}

		Stream<String> result = authorities.stream().map(GrantedAuthority::getAuthority);
		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	public static List<GrantedAuthority> extractAuthorityFromClaims(Map<String, Object> claims) {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);

		List<GrantedAuthority> result = mapRolesToGrantedAuthorities(getRolesFromClaims(claims));

		for(GrantedAuthority grantedAuthority : result) {
			LOGGER.debug("grantedAuthority={}", grantedAuthority);
		}

		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	//@SuppressWarnings("unchecked")
	private static Collection<String> getRolesFromClaims(Map<String, Object> claims) {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);

		LOGGER.info("OKTA Claims keys: {}", claims.keySet());
		LOGGER.info("Claims: {}", claims.entrySet().stream()
				.map(e -> e.getKey() + "=" + e.getValue())
				.collect(Collectors.joining(", ")));

		String claimsValue = (String)claims.getOrDefault("groups", claims.getOrDefault("roles", ""));
		LOGGER.debug("claimsValue={}", claimsValue);

//		Collection<String> roles = Arrays.stream(StringUtils.delimitedListToStringArray(claimsValue, ROLES_DELIMETER)).collect(Collectors.toSet());
		Collection<String> roles = new ArrayList<>();

		//TODO: REMOVE MOCKED AUTHORITIES
//		roles.add("MNGR");
//		roles.add("RVWR");
//		roles.add("APRV");
//		roles.add("ADTR");
		roles.add(userRole);

		LOGGER.debug("roles={}", roles);

		LOGGER.debug(LOGGER_MSG_END);
		return roles;
	}

	private static List<GrantedAuthority> mapRolesToGrantedAuthorities(Collection<String> roles) {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);

		List<GrantedAuthority> result = roles.stream()
//			.filter(role -> role.startsWith(ROLES_PREFIX))
			.map(SimpleGrantedAuthority::new)
			.collect(Collectors.toList());

		for(GrantedAuthority grantedAuthority : result) {
			LOGGER.debug("grantedAuthority={}", grantedAuthority);
		}

		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}
}
