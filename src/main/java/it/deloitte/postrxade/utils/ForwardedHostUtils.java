package it.deloitte.postrxade.utils;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Utility per ottenere l'host "logico" della richiesta quando il BE è dietro un proxy (es. ALB).
 * Ordine: X-Forwarded-Host → Host → getServerName().
 * Così nexi-be.posdatareporting.deloitte.it / amex-be.posdatareporting.deloitte.it sono usati
 * per tenant, redirect e redirect_uri OAuth invece dell'host del backend (es. App Runner).
 */
public final class ForwardedHostUtils {

    private static final String X_FORWARDED_HOST = "X-Forwarded-Host";
    private static final String HEADER_HOST = "Host";

    private ForwardedHostUtils() {
    }

    /**
     * Restituisce l'host da usare per tenant, redirect al FE e redirect_uri OAuth.
     * Senza porta (strip della parte :port se presente).
     */
    public static String getHostFromRequest(HttpServletRequest request) {
        String host = request.getHeader(X_FORWARDED_HOST);
        if (host != null && !host.isBlank()) {
            return stripPort(host.strip());
        }
        host = request.getHeader(HEADER_HOST);
        if (host != null && !host.isBlank()) {
            return stripPort(host.strip());
        }
        return request.getServerName();
    }

    private static String stripPort(String host) {
        int colon = host.indexOf(':');
        return colon > 0 ? host.substring(0, colon).trim() : host;
    }
}
