package it.deloitte.postrxade.tenant;

import it.deloitte.postrxade.utils.ForwardedHostUtils;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Imposta il tenant in {@link TenantContext} dall'host della richiesta <b>prima</b> che
 * {@link org.springframework.session.web.http.SessionRepositoryFilter} esegua il lookup della sessione.
 * <p>
 * Le sessioni sono salvate per tenant ({@link TenantAwareDataSource}): nexi → DB Nexi,
 * amex → DB Amex. Se il tenant non è impostato quando Spring Session cerca la sessione,
 * viene usato il bootstrap tenant (nexi). Su amex-be la sessione è nel DB Amex, quindi
 * senza questo filter non veniva trovata e il callback OAuth falliva.
 * <p>
 * Ordine: {@link Ordered#HIGHEST_PRECEDENCE} così questo filter gira prima di SessionRepositoryFilter.
 * {@link TenantIdentificationFilter} gira dopo e può rifinire il tenant dalla sessione (utente autenticato).
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TenantContextFromHostFilter extends OncePerRequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TenantContextFromHostFilter.class);

    private final TenantConfiguration tenantConfiguration;

    public TenantContextFromHostFilter(TenantConfiguration tenantConfiguration) {
        this.tenantConfiguration = tenantConfiguration;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String host = ForwardedHostUtils.getHostFromRequest(request);
        String tenantFromHost = tenantConfiguration.getTenantIdFromHost(host);
        String tenantId;
        if (tenantFromHost != null && tenantConfiguration.isTenantConfigured(tenantFromHost)) {
            // Usa id risolto (nexi/amex) per session lookup; retrocompatibilità aziendaa/aziendab
            tenantId = TenantConfiguration.resolveTenantAlias(tenantFromHost);
            LOGGER.debug("Tenant from host for session lookup: {} (host: {})", tenantId, host);
        } else if (tenantFromHost != null) {
            tenantId = tenantConfiguration.getBootstrapTenantResolved();
            LOGGER.debug("Host {} not configured, using bootstrap tenant {} for session lookup", host, tenantId);
        } else {
            tenantId = tenantConfiguration.getBootstrapTenantResolved();
            LOGGER.debug("Unrecognized host {}, using bootstrap tenant {} for session lookup", host, tenantId);
        }
        try {
            TenantContext.setTenantId(tenantId);
            filterChain.doFilter(request, response);
        } finally {
            TenantContext.clear();
        }
    }
}
