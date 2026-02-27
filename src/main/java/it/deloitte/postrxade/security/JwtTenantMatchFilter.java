package it.deloitte.postrxade.security;

import it.deloitte.postrxade.tenant.TenantConfiguration;
import it.deloitte.postrxade.tenant.TenantResolver;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Eseguito <b>dopo</b> la validazione JWT nella security chain.
 * Verifica che il tenant nel JWT (claim {@code tenant_id}) corrisponda al tenant dell'host della richiesta.
 * Evita che un utente con token amex possa accedere ai dati chiamando nexi-be.
 * <p>
 * In caso di mismatch si ritorna <b>401 Unauthorized</b> (non 403) con {@code error_code: "tenant_mismatch"},
 * così il FE può trattare la risposta come "token non valido per questo tenant", cancellare il token salvato
 * e reindirizzare al login/sso-select del tenant corrente, senza dover chiedere all'utente di pulire cache o cronologia.
 */
public class JwtTenantMatchFilter extends OncePerRequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(JwtTenantMatchFilter.class);

    private final TenantResolver tenantResolver;
    private final TenantConfiguration tenantConfiguration;

    public JwtTenantMatchFilter(TenantResolver tenantResolver, TenantConfiguration tenantConfiguration) {
        this.tenantResolver = tenantResolver;
        this.tenantConfiguration = tenantConfiguration;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !(authentication instanceof JwtAuthenticationToken)) {
            filterChain.doFilter(request, response);
            return;
        }

        String tenantFromHost = tenantConfiguration.getTenantIdFromHost(request.getServerName());
        if (tenantFromHost == null) {
            // Host non tenant-specific (es. App Runner URL): permetti
            filterChain.doFilter(request, response);
            return;
        }

        String tokenTenant = tenantResolver.resolveTenantFromAuthentication(authentication);
        if (tokenTenant == null) {
            LOGGER.warn("JWT has no tenant_id; rejecting request to tenant host {}", tenantFromHost);
            sendTenantMismatchResponse(response, request.getRequestURI());
            return;
        }
        // Workaround temporaneo: in test l'host è ancora aziendab/aziendaa mentre il JWT ha amex/nexi;
        // confrontiamo gli alias risolti (resolveTenantAlias). Da rimuovere quando gli URL saranno target (amex/nexi).
        String resolvedHost = TenantConfiguration.resolveTenantAlias(tenantFromHost);
        String resolvedToken = TenantConfiguration.resolveTenantAlias(tokenTenant);
        if (resolvedToken == null || !resolvedToken.equals(resolvedHost)) {
            LOGGER.warn("JWT tenant {} (resolved: {}) does not match host tenant {} (resolved: {}); returning 401 so FE can clear token and redirect to login.", tokenTenant, resolvedToken, tenantFromHost, resolvedHost);
            sendTenantMismatchResponse(response, request.getRequestURI());
            return;
        }

        filterChain.doFilter(request, response);
    }

    /**
     * 401 con error_code tenant_mismatch: il FE può cancellare il token e redirect a login/sso-select.
     */
    private static void sendTenantMismatchResponse(HttpServletResponse response, String path) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.getWriter().write("{\"status\":401,\"error\":\"Unauthorized\",\"error_code\":\"tenant_mismatch\",\"message\":\"Token not valid for this tenant. Please sign in again.\",\"path\":\"" + path + "\"}");
        response.getWriter().flush();
    }
}
