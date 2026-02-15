package it.deloitte.postrxade.config;

import java.util.List;

import jakarta.servlet.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.CollectionUtils;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;

/**
 * Configuration of web application with Servlet 3.0 APIs.
 */
@Configuration
@org.springframework.context.annotation.Profile("!batch")
public class WebConfig implements ServletContextInitializer {

	private static final Logger LOGGER = LoggerFactory.getLogger(WebConfig.class);
	private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
	private static final String LOGGER_MSG_END = "Fine";

	@Value("${application.cors.allowed-origins:null}")
	private List<String> configCorsAllowedOrigins;

	@Value("${application.cors.allowed-methods:null}")
	private List<String> configCorsAllowedMethods;

	@Value("${application.cors.allowed-headers:null}")
	private List<String> configCorsAllowedHeaders;

	@Value("${application.cors.exposed-headers:null}")
	private List<String> configCorsExposedHeaders;

	@Value("${application.cors.allow-credentials:null}")
	private Boolean configCorsAllowedCredentials;

	@Value("${application.cors.max-age:null}")
	private Long configCorsMaxAge;

	private final Environment env;

	public WebConfig(Environment env) {
		this.env = env;
	}

	@Override
	public void onStartup(ServletContext servletContext) throws ServletException {
		if (env.getActiveProfiles().length != 0) {
			LOGGER.info("Web application configuration, using profiles: {}", (Object[]) env.getActiveProfiles());
		}

		LOGGER.info("Web application fully configured");
	}

	@Bean
	public CorsFilter corsFilter() {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
		CorsConfiguration config = new CorsConfiguration();

		if (configCorsAllowedOrigins != null) {
			LOGGER.debug("configCorsAllowedOrigins={}", configCorsAllowedOrigins);
			config.setAllowedOrigins(configCorsAllowedOrigins);
		}
		if (configCorsAllowedMethods != null) {
			LOGGER.debug("configCorsAllowedMethods={}", configCorsAllowedMethods);
			config.setAllowedMethods(configCorsAllowedMethods);
		}
		if (configCorsAllowedHeaders != null) {
			LOGGER.debug("configCorsAllowedHeaders={}", configCorsAllowedHeaders);
			config.setAllowedHeaders(configCorsAllowedHeaders);
		}
		if (configCorsExposedHeaders != null) {
			LOGGER.debug("configCorsExposedHeaders={}", configCorsExposedHeaders);
			config.setExposedHeaders(configCorsExposedHeaders);
		}
		if (configCorsAllowedCredentials != null) {
			LOGGER.debug("configCorsAllowedCredentials={}", configCorsAllowedCredentials);
			config.setAllowCredentials(configCorsAllowedCredentials);
		}
		if (configCorsMaxAge != null) {
			LOGGER.debug("configCorsMaxAge={}", configCorsMaxAge);
			config.setMaxAge(configCorsMaxAge);
		}

		if (!CollectionUtils.isEmpty(config.getAllowedOrigins())) {
			LOGGER.debug("Registering CORS filter");
			source.registerCorsConfiguration("/api/**", config);
			source.registerCorsConfiguration("/management/**", config);
			source.registerCorsConfiguration("/v2/api-docs", config);
			source.registerCorsConfiguration("/v3/api-docs", config);
			source.registerCorsConfiguration("/swagger-resources", config);
			source.registerCorsConfiguration("/swagger-ui/**", config);
		}

		LOGGER.debug(LOGGER_MSG_END);
		return new CorsFilter(source);
	}
}
