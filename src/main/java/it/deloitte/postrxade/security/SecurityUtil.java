package it.deloitte.postrxade.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	private static final String PREFFER_USERNAME = "preferred_username";

	// Claim keys for roles/authorities
	private static final String CLAIM_GROUPS = "groups";
	private static final String CLAIM_ROLES = "roles";

	// Microsoft Entra authority prefixes to filter
	private static final String AUTHORITY_PREFIX_SCOPE = "SCOPE_";
	private static final String AUTHORITY_PREFIX_OIDC = "OIDC_";

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

		List<GrantedAuthority> result = mapRolesToGrantedAuthorities(getRolesFromClaims(claims, null));

		for(GrantedAuthority grantedAuthority : result) {
			LOGGER.debug("grantedAuthority={}", grantedAuthority);
		}

		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	/**
	 * Estrae i ruoli/authorities dai claims o dalle Granted Authorities.
	 * Supporta entrambi i sistemi:
	 * - Nexi: ruoli in "groups" nei claims
	 * - Microsoft Entra: ruoli nelle Granted Authorities (filtra SCOPE_* e OIDC_*)
	 *
	 * @param claims i claims del token
	 * @param grantedAuthorities le Granted Authorities del token (opzionale, per Microsoft Entra)
	 * @return collezione di ruoli/authorities
	 */
	public static List<GrantedAuthority> extractAuthorityFromClaimsAndAuthorities(Map<String, Object> claims, Collection<? extends GrantedAuthority> grantedAuthorities) {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);

		List<GrantedAuthority> result = mapRolesToGrantedAuthorities(getRolesFromClaims(claims, grantedAuthorities));

		for(GrantedAuthority grantedAuthority : result) {
			LOGGER.debug("grantedAuthority={}", grantedAuthority);
		}

		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	//@SuppressWarnings("unchecked")
	private static Collection<String> getRolesFromClaims(Map<String, Object> claims, Collection<? extends GrantedAuthority> grantedAuthorities) {
		LOGGER.debug(LOGGER_MSG_BEGIN_STATIC);

		Collection<String> roles = new ArrayList<>();

		//TODO: REMOVE MOCKED AUTHORITIES
//		roles.add("MNGR");
//		roles.add("RVWR");
//		roles.add("APRV");
//		roles.add("ADTR");
		//roles.add(userRole);
		// Prima prova a estrarre da "groups" (sistema Nexi) o "roles" (Entra: può mandare GUID, mappati in EntraRoleMapping)
		Object groupsValue = claims.get(CLAIM_GROUPS);
		if (groupsValue != null) {
			if (groupsValue instanceof String) {
				String groupsString = (String) groupsValue;
				if (!groupsString.isEmpty()) {
					for (String group : groupsString.split(ROLES_DELIMETER)) {
						if (!group.trim().isEmpty()) {
							roles.add(EntraRoleMapping.resolve(group.trim()));
						}
					}
				}
			} else if (groupsValue instanceof Collection) {
				@SuppressWarnings("unchecked")
				Collection<String> groupsCollection = (Collection<String>) groupsValue;
				for (String g : groupsCollection) {
					if (g != null && !g.isBlank()) {
						roles.add(EntraRoleMapping.resolve(g.trim()));
					}
				}
			}
		}

		if (roles.isEmpty()) {
			Object rolesValue = claims.get(CLAIM_ROLES);
			if (rolesValue != null) {
				if (rolesValue instanceof String) {
					String rolesString = (String) rolesValue;
					if (!rolesString.isEmpty()) {
						for (String role : rolesString.split(ROLES_DELIMETER)) {
							if (!role.trim().isEmpty()) {
								roles.add(EntraRoleMapping.resolve(role.trim()));
							}
						}
					}
				} else if (rolesValue instanceof Collection) {
					@SuppressWarnings("unchecked")
					Collection<String> rolesCollection = (Collection<String>) rolesValue;
					for (String r : rolesCollection) {
						if (r != null && !r.isBlank()) {
							roles.add(EntraRoleMapping.resolve(r.trim()));
						}
					}
				}
			}
		}

		// Se ancora vuoto: Granted Authorities (Microsoft Entra può mettere i ruoli lì)
		if (roles.isEmpty() && grantedAuthorities != null && !grantedAuthorities.isEmpty()) {
			for (GrantedAuthority authority : grantedAuthorities) {
				String authorityName = authority.getAuthority();
				if (!authorityName.startsWith(AUTHORITY_PREFIX_SCOPE) &&
						!authorityName.startsWith(AUTHORITY_PREFIX_OIDC)) {
					roles.add(EntraRoleMapping.resolve(authorityName));
				}
			}
		}

		// Fallback: nessun ruolo da token/config Entra
		if (roles.isEmpty()) {
			String defaultRole = (userRole != null && !userRole.isBlank()) ? userRole : "SPR";
			roles.add(defaultRole);
		}

		// Extract role codes from format TENANT_APP_AMBIENTE_RUOLO (e.g. NEXI_POSAPP_STG_SPR -> SPR)
		Collection<String> roleCodes = extractRoleCodesFromRoles(roles);

		LOGGER.debug(LOGGER_MSG_END);
		return roleCodes;
	}

	/**
	 * Extracts role codes (last part) from roles in format TENANT_APP_AMBIENTE_RUOLO.
	 * e.g. "NEXI_POSAPP_STG_SPR" -> "SPR"
	 *      "AMEX_POSAPP_PRD_ADTR" -> "ADTR"
	 * If role is already a simple code (e.g. "SPR"), logs a warning and uses it as is.
	 *
	 * @param roles collection of full role names
	 * @return collection of role codes (SPR, ADTR, MNGR, RVWR, APRV)
	 */
	private static Collection<String> extractRoleCodesFromRoles(Collection<String> roles) {
		Collection<String> roleCodes = new ArrayList<>();

		for (String role : roles) {
			if (role == null || role.isBlank()) {
				continue;
			}

			// Check if role is in format TENANT_APP_AMBIENTE_RUOLO (has underscores)
			int lastUnderscore = role.lastIndexOf('_');
			if (lastUnderscore > 0 && lastUnderscore < role.length() - 1) {
				String roleCode = role.substring(lastUnderscore + 1).trim();
				if (!roleCode.isBlank()) {
					roleCodes.add(roleCode);
				}
			} else {
				roleCodes.add(role.trim());
			}
		}

		return roleCodes;
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
