package it.deloitte.postrxade.security;

import it.deloitte.postrxade.tenant.TenantContext;
import it.deloitte.postrxade.tenant.TenantConfiguration;
import it.deloitte.postrxade.tenant.TenantResolver;
import it.deloitte.postrxade.utils.ForwardedHostUtils;
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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class CustomAuthenticationSuccessHandler implements AuthenticationSuccessHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(CustomAuthenticationSuccessHandler.class);
	private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
	private static final String LOGGER_MSG_END = "Fine";

	@Value("${fe-url:}")
	private String feUrl;

	@Value("${application.cors.tenant-base-domain:testpos-noprod.com}")
	private String tenantBaseDomain;

	private static final String JSON_403_TENANT_NOT_RECOGNIZED = "{\"status\":403,\"error\":\"Forbidden\",\"message\":\"Tenant not recognized\"}";
	private static final String JSON_403_TENANT_MISMATCH = "{\"status\":403,\"error\":\"Forbidden\",\"message\":\"Tenant does not match URL\"}";

	private final RedirectStrategy redirectStrategy = new DefaultRedirectStrategy();
	private final AppJwtService appJwtService;
	private final TenantResolver tenantResolver;
	private final TenantConfiguration tenantConfiguration;

	/**
	 * Constructor that accepts AppJwtService, TenantResolver and TenantConfiguration.
	 */
	public CustomAuthenticationSuccessHandler(AppJwtService appJwtService, TenantResolver tenantResolver, TenantConfiguration tenantConfiguration) {
		this.appJwtService = appJwtService;
		this.tenantResolver = tenantResolver;
		this.tenantConfiguration = tenantConfiguration;
	}

	// API

	@Override
	public void onAuthenticationSuccess(final HttpServletRequest request, final HttpServletResponse response, final Authentication authentication) throws IOException {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		String resolvedTenant = tenantResolver.resolveTenantFromAuthentication(authentication);

		// 1) Tenant from SSO must be one of the configured (allowed) tenants
		if (resolvedTenant == null || !tenantConfiguration.isTenantConfigured(resolvedTenant)) {
			LOGGER.warn("Tenant not recognized for user: {} (resolved: {})", authentication.getName(), resolvedTenant);
			response.setStatus(HttpServletResponse.SC_FORBIDDEN);
			response.setContentType("application/json");
			response.getWriter().write(JSON_403_TENANT_NOT_RECOGNIZED);
			response.getWriter().flush();
			return;
		}

		// 2) If the user is on a tenant-specific URL (e.g. nexi-be.posdatareporting.deloitte.it = Nexi), SSO tenant must match it.
		// On non-tenant URLs (e.g. *.awsapprunner.com) tenantFromHost is null → we skip this check.
		// Host logico (X-Forwarded-Host quando dietro proxy) per match con dominio reale.
		String host = ForwardedHostUtils.getHostFromRequest(request);
		String tenantFromHost = tenantConfiguration.getTenantIdFromHost(host);
		if (tenantFromHost == null) {
			LOGGER.info("Login on non-tenant-specific host '{}': URL tenant check skipped, using SSO tenant: {}", host, resolvedTenant);
		} else if (!resolvedTenant.equals(TenantConfiguration.resolveTenantAlias(tenantFromHost))) {
			LOGGER.warn("Tenant mismatch: URL tenant={}, SSO tenant={} for user: {}", tenantFromHost, resolvedTenant, authentication.getName());
			response.setStatus(HttpServletResponse.SC_FORBIDDEN);
			response.setContentType("application/json");
			response.getWriter().write(JSON_403_TENANT_MISMATCH);
			response.getWriter().flush();
			return;
		}

		HttpSession session = request.getSession(true);
		session.setAttribute(TenantContext.SESSION_ATTRIBUTE_TENANT_ID, resolvedTenant);
		LOGGER.info("Session bound to tenant: {} for user: {}", resolvedTenant, authentication.getName());

		handle(request, response, authentication, resolvedTenant);
		clearAuthenticationAttributes(request);

		LOGGER.debug(LOGGER_MSG_END);
	}

	// IMPL

	protected void handle(final HttpServletRequest request, final HttpServletResponse response, final Authentication authentication, String tenantId) throws IOException {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
		String targetUrl = determineTargetUrl(request, authentication);
		// Aggiungi il token JWT nel fragment così il FE può leggerlo e inviarlo come Bearer nelle API.
		// Il fragment non viene inviato al server (né in Referer) e non finisce nei log.
		// Include tenant_id nel token per supporto multi-tenant.
		String token = appJwtService.createToken(authentication, tenantId);
		targetUrl = appendTokenToUrl(targetUrl, token);
		LOGGER.info("Redirecting to FE with token in fragment (targetUrl length={})", targetUrl.length());

		if (response.isCommitted()) {
			LOGGER.warn("Response has already been committed. Unable to redirect to {}", targetUrl);
			LOGGER.debug(LOGGER_MSG_END);
			return;
		}

		redirectStrategy.sendRedirect(request, response, targetUrl);
		LOGGER.debug(LOGGER_MSG_END);
	}

	private static String appendTokenToUrl(String baseUrl, String token) {
		try {
			String fragment = "token=" + URLEncoder.encode(token, StandardCharsets.UTF_8.name());
			// Fragment dopo il primo # (il redirect Location può includere il fragment; il browser lo preserva)
			return baseUrl + "#" + fragment;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException("UTF-8 not supported", e);
		}
	}

	protected String determineTargetUrl(final HttpServletRequest request, final Authentication authentication) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		// Redirect allo stesso tenant da cui è partito il login: host amex-be.xxx → FE amex.xxx (non nexi).
		// Host logico (X-Forwarded-Host dietro proxy) così in prod si torna a nexi/amex.posdatareporting.deloitte.it.
		String host = ForwardedHostUtils.getHostFromRequest(request);
		String tenantFromHost = tenantConfiguration.getTenantIdFromHost(host);
		if (tenantFromHost != null && tenantConfiguration.isTenantConfigured(tenantFromHost)) {
			String scheme = forwardedProtoOrScheme(request);
			String feBase = scheme + "://" + tenantFromHost + "." + (tenantBaseDomain != null ? tenantBaseDomain : "testpos-noprod.com");
			LOGGER.info("Post-login redirect to same tenant FE: {} (host was {})", feBase, host);
			LOGGER.debug(LOGGER_MSG_END);
			return feBase;
		}

		// Host non tenant-specific (es. localhost, *.awsapprunner.com): usa fe-url da config
		if (feUrl != null && !feUrl.isBlank()) {
			LOGGER.debug(LOGGER_MSG_END);
			return feUrl;
		}
		String requestUrl = request.getRequestURL().toString();
		if (requestUrl.contains(":8082")) {
			String frontendUrl = "http://localhost:8080";
			LOGGER.debug("Request came to BE (local), redirecting to FE: {}", frontendUrl);
			LOGGER.debug(LOGGER_MSG_END);
			return frontendUrl;
		}

		String result = "/";
		LOGGER.debug("Using relative redirect: {}", result);
		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	private static String forwardedProtoOrScheme(HttpServletRequest request) {
		String proto = request.getHeader("X-Forwarded-Proto");
		if (proto != null && !proto.isBlank()) {
			return proto.strip().toLowerCase();
		}
		return request.getScheme();
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
