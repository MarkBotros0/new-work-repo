package it.deloitte.postrxade.tenant;

import it.deloitte.postrxade.security.EntraRoleMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Resolves the tenant identifier from the authentication token.
 * <ul>
 *   <li>First tries the {@code tenant_id} claim.</li>
 *   <li>Then derives from role names: e.g. {@code NEXI_POSAPP_STG_SPR} → prefix before first "_"
 *       mapped to configured tenant ids (NEXI→nexi, AMEX→amex).</li>
 * </ul>
 * This allows the SSO to bind the session to a tenant via roles like {@code NEXI_POSAPP_STG_SPR}.
 */
@Component
public class TenantResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(TenantResolver.class);
    private static final String TENANT_ID_CLAIM = "tenant_id";
    private static final String CLAIM_GROUPS = "groups";
    private static final String CLAIM_ROLES = "roles";
    private static final String ROLES_DELIMITER = " ";
    private static final String LOCAL_FALLBACK_ROLE = "NEXI_POSAPP_STG_SPR";
    private static final Pattern POSAPP_ROLE_PATTERN = Pattern.compile("^[A-Z]+_POSAPP_[A-Z]+_[A-Z]+$");

    private final TenantConfiguration tenantConfiguration;
    @Value("${application.security.local-sso-workaround-enabled:false}")
    private boolean localSsoWorkaroundEnabled;

    public TenantResolver(TenantConfiguration tenantConfiguration) {
        this.tenantConfiguration = tenantConfiguration;
    }

    /**
     * Resolves the tenant id from the authentication (token claims and/or role prefixes).
     *
     * @param authentication the current authentication
     * @return the tenant id (nexi=Nexi, amex=Amex), or null if not determinable
     */
    public String resolveTenantFromAuthentication(Authentication authentication) {
        if (authentication == null || !authentication.isAuthenticated()
                || "anonymousUser".equals(authentication.getPrincipal())) {
            return null;
        }

        // 1) Claim tenant_id
        String fromClaim = extractTenantIdFromClaims(authentication);
        if (fromClaim != null) {
            if (tenantConfiguration.isTenantConfigured(fromClaim)) {
                LOGGER.debug("Resolved tenant from claim tenant_id: {}", fromClaim);
                return fromClaim;
            }
            LOGGER.warn("tenant_id claim value '{}' is not a configured tenant", fromClaim);
        }

        // 2) From role prefix (NEXI_* -> nexi, AMEX_* -> amex)
        // Format: TENANT_APP_AMBIENTE_RUOLO. We use only the first role for tenant resolution.
        Set<String> configuredTenantIds = tenantConfiguration.getTenants().keySet();
        List<String> roleNames = getRoleNamesFromAuthentication(authentication);
        if (roleNames.isEmpty()) {
            LOGGER.warn("No roles from authentication; cannot resolve tenant");
            return null;
        }
        String firstRole = roleNames.get(0);
        // Entra ID sends GUIDs; map to TENANT_APP_AMBIENTE_RUOLO before extracting tenant
        String roleForTenant = EntraRoleMapping.resolve(firstRole);
        String tenantFromRole = extractTenantFromRole(roleForTenant);
        if (tenantFromRole != null && configuredTenantIds.contains(tenantFromRole)) {
            LOGGER.debug("Resolved tenant from first role: {} -> {}", roleForTenant, tenantFromRole);
            return tenantFromRole;
        }
        LOGGER.warn("Tenant not recognized: first role '{}' (mapped: '{}') -> tenant '{}' not in configured tenants {}", firstRole, roleForTenant, tenantFromRole, configuredTenantIds);
        return null;
    }

    private String extractTenantIdFromClaims(Authentication authentication) {
        Map<String, Object> claims = getClaims(authentication);
        if (claims != null && claims.containsKey(TENANT_ID_CLAIM)) {
            Object v = claims.get(TENANT_ID_CLAIM);
            return v != null ? v.toString().trim() : null;
        }
        return null;
    }

    private List<String> getRoleNamesFromAuthentication(Authentication authentication) {
        List<String> roles = new ArrayList<>();

        Map<String, Object> claims = getClaims(authentication);
        if (claims != null) {
            Object groupsValue = claims.get(CLAIM_GROUPS);
            if (groupsValue instanceof String) {
                for (String s : ((String) groupsValue).split(ROLES_DELIMITER)) {
                    if (s != null && !s.isBlank()) roles.add(s.trim());
                }
            } else if (groupsValue instanceof Collection) {
                for (Object o : (Collection<?>) groupsValue) {
                    if (o != null) roles.add(o.toString().trim());
                }
            }
            Object rolesValue = claims.get(CLAIM_ROLES);
            if (rolesValue != null) {
                if (rolesValue instanceof String) {
                    for (String s : ((String) rolesValue).split(ROLES_DELIMITER)) {
                        if (s != null && !s.isBlank()) roles.add(s.trim());
                    }
                } else if (rolesValue instanceof Collection) {
                    for (Object o : (Collection<?>) rolesValue) {
                        if (o != null) roles.add(o.toString().trim());
                    }
                }
            }
        }

        if (roles.isEmpty() && authentication.getAuthorities() != null) {
            for (GrantedAuthority a : authentication.getAuthorities()) {
                String name = a.getAuthority();
                if (name != null && !name.startsWith("SCOPE_") && !name.startsWith("OIDC_")) {
                    roles.add(name);
                }
            }
        }

        if (localSsoWorkaroundEnabled) {
            List<String> filteredRoles = roles.stream()
                    .filter(Objects::nonNull)
                    .map(String::trim)
                    .filter(r -> !r.isBlank())
                    .map(EntraRoleMapping::resolve)
                    .filter(r -> POSAPP_ROLE_PATTERN.matcher(r).matches())
                    .toList();

            if (!filteredRoles.isEmpty()) {
                roles = new ArrayList<>(filteredRoles);
            } else {
                roles = new ArrayList<>();
                roles.add(LOCAL_FALLBACK_ROLE);
                LOGGER.warn("LOCAL SSO WORKAROUND ACTIVE: no POSAPP role found in token, injecting fallback role {}", LOCAL_FALLBACK_ROLE);
            }
        }

        return roles;
    }

    /**
     * Extracts the tenant identifier from a role in format TENANT_APP_AMBIENTE_RUOLO.
     * Maps SSO tenant codes to internal tenant IDs: NEXI -> nexi, AMEX -> amex.
     *
     * @param role the role name (e.g. "NEXI_POSAPP_STG_SPR" or "AMEX_POSAPP_PRD_ADTR")
     * @return the tenant ID (nexi or amex) or null if not recognized
     */
    private static String extractTenantFromRole(String role) {
        if (role == null || role.isBlank()) return null;
        
        // Extract first part before underscore (TENANT)
        int firstUnderscore = role.indexOf('_');
        if (firstUnderscore < 0) return null;
        
        String tenantCode = role.substring(0, firstUnderscore).trim();
        if (tenantCode.isBlank()) return null;
        
        // Map SSO tenant codes to internal tenant IDs (nomi veri: nexi, amex)
        String tenantUpper = tenantCode.toUpperCase();
        switch (tenantUpper) {
            case "NEXI":
                return "nexi";
            case "AMEX":
                return "amex";
            default:
                // Fallback: try lowercase match with configured tenants
                return tenantCode.toLowerCase();
        }
    }
    
    /**
     * Extracts the role code (last part) from a role in format TENANT_APP_AMBIENTE_RUOLO.
     * e.g. "NEXI_POSAPP_STG_SPR" -> "SPR"
     *      "AMEX_POSAPP_PRD_ADTR" -> "ADTR"
     * 
     * @param role the full role name
     * @return the role code (e.g. "SPR", "ADTR", "MNGR") or null if not in expected format
     */
    public static String extractRoleCodeFromRole(String role) {
        if (role == null || role.isBlank()) return null;
        
        int lastUnderscore = role.lastIndexOf('_');
        if (lastUnderscore < 0 || lastUnderscore >= role.length() - 1) return null;
        
        String roleCode = role.substring(lastUnderscore + 1).trim();
        return roleCode.isBlank() ? null : roleCode;
    }

    private static Map<String, Object> getClaims(Authentication authentication) {
        if (authentication instanceof OAuth2AuthenticationToken) {
            if (authentication.getPrincipal() instanceof OidcUser oidcUser) {
                return oidcUser.getClaims();
            }
        } else if (authentication instanceof JwtAuthenticationToken jwtToken) {
            return jwtToken.getToken().getClaims();
        }
        return null;
    }
}
