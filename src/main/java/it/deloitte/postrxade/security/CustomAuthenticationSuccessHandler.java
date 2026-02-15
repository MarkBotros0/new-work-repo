package it.deloitte.postrxade.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.DefaultRedirectStrategy;
import org.springframework.security.web.RedirectStrategy;
import org.springframework.security.web.WebAttributes;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.io.IOException;
import java.util.List;

public class CustomAuthenticationSuccessHandler implements AuthenticationSuccessHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(CustomAuthenticationSuccessHandler.class);
	private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
	private static final String LOGGER_MSG_END = "Fine";

	@Value("${fe-url:null}")
	private String FE_URL;

	private RedirectStrategy redirectStrategy = new DefaultRedirectStrategy();

	public CustomAuthenticationSuccessHandler() {
		super();
	}

	// API

	@Override
	public void onAuthenticationSuccess(final HttpServletRequest request, final HttpServletResponse response, final Authentication authentication) throws IOException {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		handle(request, response, authentication);
		clearAuthenticationAttributes(request);

		LOGGER.debug(LOGGER_MSG_END);
	}

	// IMPL

	protected void handle(final HttpServletRequest request, final HttpServletResponse response, final Authentication authentication) throws IOException {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
		final String targetUrl = determineTargetUrl(request, authentication);
		LOGGER.info("targetUrl={}", targetUrl);

		if (response.isCommitted()) {
			LOGGER.warn("Response has already been committed. Unable to redirect to {}", targetUrl);
			LOGGER.debug(LOGGER_MSG_END);
			return;
		}

		redirectStrategy.sendRedirect(request, response, targetUrl);
		LOGGER.debug(LOGGER_MSG_END);
	}

	protected String determineTargetUrl(final HttpServletRequest request, final Authentication authentication) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		// Map<String, String> roleTargetUrlMap = new HashMap<>();
		// roleTargetUrlMap.put("ROLE_USER", "/homepage.html");
		// roleTargetUrlMap.put("ROLE_ADMIN", "/console.html");

		// final Collection<? extends GrantedAuthority> authorities = authentication.getAuthorities();
		// for (final GrantedAuthority grantedAuthority : authorities) {

		// 	String authorityName = grantedAuthority.getAuthority();
		// 	if(roleTargetUrlMap.containsKey(authorityName)) {
		// 		LOGGER.debug(LOGGER_MSG_END);
		// 		return roleTargetUrlMap.get(authorityName);
		// 	}
		// }

		// throw new IllegalStateException();

		// Since redirect-uri is configured as http://localhost:8080/api/login/oauth2/code/oidc,
		// the callback always comes to the FE first, which proxies it to the BE.
		// We need to redirect back to the FE homepage (http://localhost:8080) instead of
		// using a relative redirect which would go to the BE (http://localhost:8082/).
		String requestUrl = request.getRequestURL().toString();
		

		if (!FE_URL.isBlank()) { //If the FE-url is populated
			return FE_URL;
		} else if (requestUrl.contains(":8082")) {// If the request came to the BE (port 8082), redirect to FE (port 8080)
			String frontendUrl = "http://localhost:8080";
			LOGGER.debug("Request came to BE, redirecting to FE: {}", frontendUrl);
			LOGGER.debug(LOGGER_MSG_END);
			return frontendUrl;
		}
		
		// Fallback: relative redirect (should not happen if redirect-uri is correctly configured)
		String result = "/";
		LOGGER.debug("Using relative redirect: {}", result);
		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	/**
	 * Removes temporary authentication-related data which may have been stored in the session
	 * during the authentication process.
	 */
	protected final void clearAuthenticationAttributes(final HttpServletRequest request) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		final HttpSession session = request.getSession(false);

		if (session == null) {
			LOGGER.debug(LOGGER_MSG_END);
			return;
		}

		session.removeAttribute(WebAttributes.AUTHENTICATION_EXCEPTION);
		LOGGER.debug(LOGGER_MSG_END);
	}

}
