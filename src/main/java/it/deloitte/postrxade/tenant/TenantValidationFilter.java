package it.deloitte.postrxade.tenant;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Validates that the tenant from the token (claim or role prefix) matches the current
 * tenant context (from session or host). For authenticated users, the session tenant is
 * set at login from the SSO token; this filter ensures token and context stay aligned.
 */
@Component
@Order(2)
public class TenantValidationFilter extends OncePerRequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TenantValidationFilter.class);

    private final TenantResolver tenantResolver;

    public TenantValidationFilter(TenantResolver tenantResolver) {
        this.tenantResolver = tenantResolver;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null && authentication.isAuthenticated()
                && !"anonymousUser".equals(authentication.getPrincipal())) {

            String currentTenantId = TenantContext.getTenantId();

            if (currentTenantId == null) {
                LOGGER.warn("No tenant context available for validation");
                filterChain.doFilter(request, response);
                return;
            }

            String tokenTenantId = tenantResolver.resolveTenantFromAuthentication(authentication);

            if (tokenTenantId != null && !currentTenantId.equals(tokenTenantId)) {
                LOGGER.error("Tenant mismatch: context={}, token={}, user={}",
                        currentTenantId, tokenTenantId, authentication.getName());

                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                response.setContentType("application/json");
                response.getWriter().write("{\"status\":403,\"error\":\"Forbidden\",\"message\":\"Tenant mismatch: user does not belong to this tenant\",\"path\":\"" + request.getRequestURI() + "\"}");
                response.getWriter().flush();
                return;
            }

            if (tokenTenantId == null) {
                LOGGER.warn("Could not resolve tenant from token for user: {}. Ensure token has tenant_id claim or role prefix (e.g. NEXI_POSAPP_*).",
                        authentication.getName());
            } else {
                LOGGER.debug("Tenant validation passed: {} for user: {}", currentTenantId, authentication.getName());
            }
        }

        filterChain.doFilter(request, response);
    }
}
