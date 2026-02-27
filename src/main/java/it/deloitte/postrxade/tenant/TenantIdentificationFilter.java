package it.deloitte.postrxade.tenant;

import it.deloitte.postrxade.utils.ForwardedHostUtils;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Sets the tenant for the request in TenantContext.
 * <ul>
 *   <li>If the session has a tenant (set at login from SSO token), that is used — the session is the source of truth for authenticated users.</li>
 *   <li>Otherwise the tenant is derived from the request host (for unauthenticated users, e.g. SSO selection page).</li>
 *   <li>If the host is unrecognized (e.g. single App Runner URL), checks optional header {@value #HEADER_TENANT_ID}; if valid, uses it; else bootstrap (nexi).</li>
 *   <li>If the host is recognized and differs from the session tenant, the request is rejected (403) to avoid using the wrong URL.</li>
 * </ul>
 * URL patterns: nexi*.testpos-noprod.com (Nexi), amex*.testpos-noprod.com (Amex). See docs/env-and-deployment.md.
 */
@Component
@Order(0)
public class TenantIdentificationFilter extends OncePerRequestFilter {

    /** Header opzionale per indicare il tenant quando l'host non lo identifica (es. X-Tenant-Id: amex). */
    public static final String HEADER_TENANT_ID = "X-Tenant-Id";
    /** Query param opzionale per il tenant (es. ?tenant=amex), utile per test da browser con URL generico App Runner. */
    public static final String PARAM_TENANT = "tenant";

    private static final Logger LOGGER = LoggerFactory.getLogger(TenantIdentificationFilter.class);

    private final TenantConfiguration tenantConfiguration;

    public TenantIdentificationFilter(TenantConfiguration tenantConfiguration) {
        this.tenantConfiguration = tenantConfiguration;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String host = ForwardedHostUtils.getHostFromRequest(request);
        LOGGER.debug("Identifying tenant, host: {}", host);

        try {
            HttpSession session = request.getSession(false);
            String tenantFromSession = session != null ? (String) session.getAttribute(TenantContext.SESSION_ATTRIBUTE_TENANT_ID) : null;
            String tenantFromHost = tenantConfiguration.getTenantIdFromHost(host);

            String tenantId;
            if (tenantFromSession != null && tenantConfiguration.isTenantConfigured(tenantFromSession)) {
                tenantId = tenantFromSession;
                LOGGER.debug("Using tenant from session: {}", tenantId);
                // Confronto con alias risolti: aziendaa↔nexi e aziendab↔amex così host vecchi non danno 403
                String hostResolved = tenantFromHost != null ? TenantConfiguration.resolveTenantAlias(tenantFromHost) : null;
                String sessionResolved = TenantConfiguration.resolveTenantAlias(tenantId);
                if (hostResolved != null && !hostResolved.equals(sessionResolved)) {
                    LOGGER.warn("Host tenant {} differs from session tenant {}; rejecting.", tenantFromHost, tenantId);
                    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    response.setContentType("application/json");
                    response.getWriter().write("{\"status\":403,\"error\":\"Forbidden\",\"message\":\"Tenant does not match session\",\"path\":\"" + request.getRequestURI() + "\"}");
                    response.getWriter().flush();
                    return;
                }
            } else {
                tenantId = tenantFromHost;
                if (tenantId == null) {
                    // Host non riconosciuto: prova header X-Tenant-Id, poi query param "tenant" (per test da browser con URL unico)
                    String headerTenant = request.getHeader(HEADER_TENANT_ID);
                    String paramTenant = request.getParameter(PARAM_TENANT);
                    String explicitTenant = headerTenant != null && !headerTenant.isBlank() ? headerTenant.trim() : (paramTenant != null && !paramTenant.isBlank() ? paramTenant.trim() : null);
                    if (explicitTenant != null && tenantConfiguration.isTenantConfigured(explicitTenant)) {
                        tenantId = TenantConfiguration.resolveTenantAlias(explicitTenant);
                        LOGGER.info("Unrecognized host: {}, using tenant from {}: {}", host, headerTenant != null ? HEADER_TENANT_ID : PARAM_TENANT, tenantId);
                    } else {
                        tenantId = tenantConfiguration.getBootstrapTenantResolved();
                        LOGGER.info("Unrecognized host: {}, using bootstrap tenant: {} (set {} or {} for another tenant)", host, tenantId, HEADER_TENANT_ID, PARAM_TENANT);
                    }
                } else if (!tenantConfiguration.isTenantConfigured(tenantId)) {
                    LOGGER.warn("Tenant {} from host {} is not configured, falling back to bootstrap tenant", tenantId, host);
                    tenantId = tenantConfiguration.getBootstrapTenantResolved();
                } else {
                    // Normalizza così in contesto/sessione resta nexi/amex (retrocompatibilità URL aziendaa/aziendab)
                    tenantId = TenantConfiguration.resolveTenantAlias(tenantId);
                    LOGGER.info("Tenant from host: {} for host: {}", tenantId, host);
                }
            }

            // Final validation: ensure we have a valid tenant (usa id risolto)
            if (tenantId == null || !tenantConfiguration.isTenantConfigured(tenantId)) {
                tenantId = tenantConfiguration.getBootstrapTenantResolved();
                LOGGER.error("No valid tenant found, using bootstrap tenant: {} for host: {}", tenantId, host);
            }

            TenantContext.setTenantId(tenantId);
            filterChain.doFilter(request, response);
        } finally {
            TenantContext.clear();
        }
    }
}
