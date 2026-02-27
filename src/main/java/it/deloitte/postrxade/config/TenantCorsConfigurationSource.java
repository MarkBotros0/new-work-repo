package it.deloitte.postrxade.config;

import it.deloitte.postrxade.tenant.TenantConfiguration;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * CORS configuration per tenant: ogni BE (es. amex-be.testpos-noprod.com) accetta
 * solo le origini del proprio tenant (FE + BE), non quelle degli altri tenant.
 * Per host non tenant-specific (es. *.awsapprunner.com) si ammettono tutte le origini configurate.
 */
public class TenantCorsConfigurationSource implements CorsConfigurationSource {

	private static final List<String> LOCALHOST_ORIGINS = List.of(
		"http://localhost:8080", "http://localhost:8082", "http://localhost:3000", "http://localhost:4200"
	);

	private final String corsTenantBaseDomain;
	private final TenantConfiguration tenantConfiguration;
	private final List<String> extraOrigins;
	private final CorsConfiguration baseConfig;

	/** Origini "fallback" quando l'host non è tenant-specific (es. App Runner generico). */
	private final List<String> allTenantOrigins;

	public TenantCorsConfigurationSource(
		String corsTenantBaseDomain,
		TenantConfiguration tenantConfiguration,
		List<String> extraOrigins,
		CorsConfiguration baseConfig
	) {
		this.corsTenantBaseDomain = corsTenantBaseDomain;
		this.tenantConfiguration = tenantConfiguration;
		this.extraOrigins = extraOrigins != null ? extraOrigins : List.of();
		this.baseConfig = baseConfig;

		Set<String> all = new LinkedHashSet<>(LOCALHOST_ORIGINS);
		if (StringUtils.hasText(corsTenantBaseDomain) && tenantConfiguration.getTenants() != null) {
			String base = corsTenantBaseDomain.trim().toLowerCase();
			if (base.startsWith(".")) {
				base = base.substring(1);
			}
			for (String tenantId : tenantConfiguration.getTenants().keySet()) {
				all.add("https://" + tenantId + "." + base);
				all.add("https://" + tenantId + "-be." + base);
			}
		}
		all.addAll(extraOrigins);
		this.allTenantOrigins = new ArrayList<>(all);
	}

	private static boolean isCorsPath(String path) {
		return path != null && (
			path.startsWith("/api/") || path.startsWith("/management/") ||
			path.equals("/v2/api-docs") || path.equals("/v3/api-docs") ||
			path.startsWith("/swagger-resources") || path.startsWith("/swagger-ui/")
		);
	}

	@Override
	public CorsConfiguration getCorsConfiguration(HttpServletRequest request) {
		if (request == null || !isCorsPath(request.getRequestURI())) {
			return null;
		}
		String host = request.getHeader("Host");
		if (host != null && host.contains(":")) {
			host = host.substring(0, host.indexOf(':'));
		}

		List<String> allowedOrigins;
		if (!StringUtils.hasText(host) || host.contains("localhost") || host.contains("127.0.0.1")) {
			allowedOrigins = new ArrayList<>(LOCALHOST_ORIGINS);
			allowedOrigins.addAll(extraOrigins);
		} else {
			String tenantId = tenantConfiguration.getTenantIdFromHost(host);
			if (tenantId != null) {
				// Per-tenant: solo FE e BE di questo tenant
				String base = corsTenantBaseDomain.trim().toLowerCase();
				if (base.startsWith(".")) {
					base = base.substring(1);
				}
				allowedOrigins = List.of(
					"https://" + tenantId + "." + base,
					"https://" + tenantId + "-be." + base
				);
			} else {
				// Host non tenant-specific (es. *.awsapprunner.com): ammettere tutte le origini configurate
				allowedOrigins = allTenantOrigins;
			}
		}

		if (CollectionUtils.isEmpty(allowedOrigins)) {
			return null;
		}
		CorsConfiguration config = new CorsConfiguration();
		config.setAllowedOrigins(allowedOrigins);
		if (baseConfig.getAllowedMethods() != null) {
			config.setAllowedMethods(baseConfig.getAllowedMethods());
		}
		if (baseConfig.getAllowedHeaders() != null) {
			config.setAllowedHeaders(baseConfig.getAllowedHeaders());
		}
		if (baseConfig.getExposedHeaders() != null) {
			config.setExposedHeaders(baseConfig.getExposedHeaders());
		}
		if (baseConfig.getAllowCredentials() != null) {
			config.setAllowCredentials(baseConfig.getAllowCredentials());
		}
		if (baseConfig.getMaxAge() != null) {
			config.setMaxAge(baseConfig.getMaxAge());
		}
		return config;
	}
}
