package it.deloitte.postrxade.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Filter that intercepts requests to /api/authorization/oidc and redirects
 * to the SSO selection page if the request doesn't contain OAuth2 parameters.
 * <p>
 * This allows users to see a selection page before being redirected to the
 * OAuth2 authorization server.
 */
@Component
@Order(1)
@org.springframework.context.annotation.Profile("!batch")
public class SsoSelectionFilter extends OncePerRequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SsoSelectionFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String requestURI = request.getRequestURI();
        String queryString = request.getQueryString();

        // Check if this is a request to /api/authorization/oidc without OAuth2 parameters
        if ("/api/authorization/oidc".equals(requestURI)) {
            // Check if this is an OAuth2 callback or a request with OAuth2 parameters
            boolean isOAuth2Request = queryString != null && (
                queryString.contains("response_type") ||
                queryString.contains("client_id") ||
                queryString.contains("redirect_uri") ||
                queryString.contains("state") ||
                queryString.contains("code")
            );

            // Check if this is a request from the SSO selection page (has provider parameter)
            boolean isFromSsoSelection = queryString != null && queryString.contains("provider=nexiauth");

            if (!isOAuth2Request && !isFromSsoSelection) {
                // This is a direct request to the authorization endpoint without OAuth2 parameters
                // and not from the SSO selection page - redirect to the SSO selection page
                LOGGER.debug("Redirecting to SSO selection page for request: {}", requestURI);
                response.sendRedirect("/sso-select");
                return;
            }
            
            // If it's from SSO selection page, let Spring Security handle it (it will ignore the provider parameter)
        }

        // Continue with the filter chain
        filterChain.doFilter(request, response);
    }
}

