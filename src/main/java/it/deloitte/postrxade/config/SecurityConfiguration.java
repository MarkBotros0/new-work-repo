package it.deloitte.postrxade.config;


import it.deloitte.postrxade.exception.AuthorityCodeNotValidException;
import it.deloitte.postrxade.security.CustomAuthenticationSuccessHandler;
import it.deloitte.postrxade.security.SsoSelectionFilter;

import it.deloitte.postrxade.security.SecurityUtil;
import it.deloitte.postrxade.security.oauth.*;
import it.deloitte.postrxade.service.AuthorityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.authority.mapping.GrantedAuthoritiesMapper;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.oauth2.client.endpoint.DefaultAuthorizationCodeTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.OAuth2AccessTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.OAuth2AuthorizationCodeGrantRequest;
import org.springframework.security.oauth2.client.http.OAuth2ErrorResponseErrorHandler;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.*;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;
import org.springframework.security.oauth2.core.http.converter.OAuth2AccessTokenResponseHttpMessageConverter;
import org.springframework.security.oauth2.core.oidc.user.OidcUserAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.csrf.CsrfFilter;
import org.springframework.security.web.session.HttpSessionEventPublisher;
import org.springframework.session.jdbc.JdbcIndexedSessionRepository;
import org.springframework.session.security.SpringSessionBackedSessionRegistry;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.filter.CorsFilter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@EnableWebSecurity(debug = false)
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true)
@Configuration
@org.springframework.context.annotation.Profile("!batch")
public class SecurityConfiguration {

	private static final Logger LOGGER = LoggerFactory.getLogger(SecurityConfiguration.class);
	private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
	private static final String LOGGER_MSG_END = "Fine";

	@Value("${spring.security.oauth2.client.provider.oidc.jwk-set-uri}")
	private String cfgJwkSetUri;

	@Value("${application.security.xsfr.enabled}")
	private boolean cfgXsfrEnabled;

	@Value("${application.security.xsfr.cookie-path}")
	private String cfgXsfrCookiePath;

	@Value("${user-role:SPR}")
	private String userRole;

	private final CorsFilter corsFilter;
	private final SsoSelectionFilter ssoSelectionFilter;

	private ClientRegistrationRepository clientRegistrationRepository;
	private final SessionRegistry sessionRegistry;

	private final AuthorityService authorityService;

	public SecurityConfiguration(
		ClientRegistrationRepository clientRegistrationRepository,
		CorsFilter corsFilter,
		SsoSelectionFilter ssoSelectionFilter,
		AuthorityService authorityService,
		SessionRegistry sessionRegistry
	) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		if (clientRegistrationRepository!=null) {
			LOGGER.debug("clientRegistrationRepository NOT NULL");
		}
		else {
			LOGGER.debug("clientRegistrationRepository NULL");
		}

		this.clientRegistrationRepository = clientRegistrationRepository;
		this.corsFilter = corsFilter;
		this.ssoSelectionFilter = ssoSelectionFilter;
		this.authorityService = authorityService;
		this.sessionRegistry = sessionRegistry;

		LOGGER.debug(LOGGER_MSG_END);
	}

	@PostConstruct
	public void initUserRole() {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
		LOGGER.info("Initializing userRole from configuration: {}", userRole);
		SecurityUtil.setUserRole(userRole);
		LOGGER.debug(LOGGER_MSG_END);
	}

	private void configureCsrf(HttpSecurity http) throws Exception {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		LOGGER.debug("cfgXsfrEnabled={}", cfgXsfrEnabled);
		if (cfgXsfrEnabled) {
			LOGGER.info("XSRF abilitato");
			CookieCsrfTokenRepository tokenRepository = CookieCsrfTokenRepository.withHttpOnlyFalse();

			LOGGER.debug("cfgXsfrCookiePath={}", cfgXsfrCookiePath);
			if (cfgXsfrCookiePath != null) {
				tokenRepository.setCookiePath(cfgXsfrCookiePath);
			}
			http
				.csrf(csrf -> csrf
					.csrfTokenRepository(tokenRepository)
					.ignoringRequestMatchers("/private-intmon/jolokia")
					.ignoringRequestMatchers("/private-intmon/jolokia/**")
				);
		}
		else {
			LOGGER.info("XSRF non abilitato");
			http.csrf(csrf -> csrf.disable());
		}

		LOGGER.debug(LOGGER_MSG_END);
	}

	@Bean
	public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		LOGGER.debug("OAuth2AuthorizationRequestRedirectFilter.DEFAULT_AUTHORIZATION_REQUEST_BASE_URI={}", OAuth2AuthorizationRequestRedirectFilter.DEFAULT_AUTHORIZATION_REQUEST_BASE_URI);

		LOGGER.debug("Call to [configureCsrf(http)]");
		configureCsrf(http);

		http
            .csrf(AbstractHttpConfigurer::disable)
			.addFilterBefore(ssoSelectionFilter, CsrfFilter.class)
			.addFilterBefore(corsFilter, CsrfFilter.class)
			.anonymous(AbstractHttpConfigurer::disable)
			.exceptionHandling(customizer -> customizer
				.authenticationEntryPoint((request, response, authException) -> {
					LOGGER.warn("Unauthorized access to: {}", request.getRequestURI());
					response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
					response.setContentType("application/json");
					response.getWriter().write("{\"status\":401,\"error\":\"Unauthorized\",\"message\":\"Authentication required\",\"path\":\"" + request.getRequestURI() + "\"}");
					response.getWriter().flush();
				})
				.accessDeniedHandler((request, response, accessDeniedException) -> {
					LOGGER.warn("Access denied to: {}", request.getRequestURI());
					response.setStatus(HttpServletResponse.SC_FORBIDDEN);
					response.setContentType("application/json");
					response.getWriter().write("{\"status\":403,\"error\":\"Forbidden\",\"message\":\"Access denied\",\"path\":\"" + request.getRequestURI() + "\"}");
					response.getWriter().flush();
				}))
		.authorizeHttpRequests(authz -> authz
			// Monitoring endpoints

			.requestMatchers("/private-intmon/health").permitAll()
			.requestMatchers("/private-intmon/health/**").permitAll()
			.requestMatchers("/private-intmon/jolokia").permitAll()
			.requestMatchers("/private-intmon/jolokia/**").permitAll()
			.requestMatchers("/private-intmon/info").permitAll()
			.requestMatchers("/actuator/**").permitAll()

			// OAuth2 endpoints
			.requestMatchers("/api/authorization/oidc").permitAll()
			.requestMatchers("/api/authorization/oidc/**").permitAll()
			.requestMatchers("/api/authorization/oidc-azure").permitAll()
			.requestMatchers("/api/login/oauth2/code/oidc").permitAll()
			.requestMatchers("/api/login/oauth2/code/oidc-azure").permitAll()
			
			// SSO selection page
			.requestMatchers("/sso-select").permitAll()

			// Public endpoints
			.requestMatchers("/").permitAll()
			.requestMatchers("/error").permitAll()
			.requestMatchers("/webjars/**").permitAll()

			// API Documentation
			.requestMatchers("/api/docs/**").permitAll()
			.requestMatchers("/swagger-ui/**").permitAll()
			.requestMatchers("/v3/api-docs/**").permitAll()

			// Test endpoints
			.requestMatchers("/api/test/**").permitAll()
			.requestMatchers("/api/health/**").permitAll()


			// User endpoints (authenticated)
			.requestMatchers("/api/user/**").authenticated()

            // All other API endpoints require authentication
            .requestMatchers("/api/**").authenticated().anyRequest().permitAll()//.denyAll()


        )
		.oauth2Login()
			.authorizationEndpoint()
				.authorizationRequestResolver(new CustomAuthorizationRequestResolver(clientRegistrationRepository))
				// .baseUri(cfgAuthorizationUri)
				.authorizationRequestRepository(authorizationRequestRepository())
				.and()
			.authorizedClientRepository(authorizedClientRepository())

			// .defaultSuccessUrl("/", true)
			.successHandler(authenticationSuccessHandler())

			.failureUrl("/login?error=1")
			.redirectionEndpoint()
				.baseUri("/api/login/oauth2/code/*")
				.and()
			.tokenEndpoint()
				.accessTokenResponseClient(accessTokenResponseClient())
				.and()
			.and()
		.oauth2ResourceServer()
			.jwt()
				.jwtAuthenticationConverter(authenticationConverter())
				.and()
			.and()
		.oauth2Client()
			.and()
		.sessionManagement()
			.maximumSessions(1)
			.expiredUrl("/expired")
			.maxSessionsPreventsLogin(false)
			.sessionRegistry(sessionRegistry)
		.and()
			.sessionCreationPolicy(SessionCreationPolicy.ALWAYS)
			.and()
			.requestCache()
				.requestCache(requestCache())
			.and()
				.headers()
				.contentSecurityPolicy("default-src 'self'; style-src 'self' 'unsafe-inline'");

		LOGGER.debug(LOGGER_MSG_END);
		return http.build();
	}

	@Bean
	public static ServletListenerRegistrationBean<HttpSessionEventPublisher> httpSessionEventPublisher() {
		return new ServletListenerRegistrationBean<>(new HttpSessionEventPublisher());
	}

	@Bean
	public static SessionRegistry sessionRegistry(JdbcIndexedSessionRepository sessionRepository) {
		return new SpringSessionBackedSessionRegistry<>(sessionRepository);
	}

	// @Bean
	public OAuth2AuthorizedClientRepository authorizedClientRepository() {
		return new HttpSessionOAuth2AuthorizedClientRepository();
	}

	@Bean
	public AuthenticationSuccessHandler authenticationSuccessHandler() {
		return new CustomAuthenticationSuccessHandler();
	}

	@Bean
	public AuthorizationRequestRepository<OAuth2AuthorizationRequest> authorizationRequestRepository() {
		return new HttpSessionOAuth2AuthorizationRequestRepository();
	}

	@Bean
	public org.springframework.security.web.savedrequest.RequestCache requestCache() {
		// Disabilita il salvataggio delle richieste non autenticate per forzare 401 invece di redirect
		return new org.springframework.security.web.savedrequest.NullRequestCache();
	}

	@Bean
	public OAuth2AccessTokenResponseClient<OAuth2AuthorizationCodeGrantRequest> accessTokenResponseClient() {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		DefaultAuthorizationCodeTokenResponseClient accessTokenResponseClient = new DefaultAuthorizationCodeTokenResponseClient();
		accessTokenResponseClient.setRequestEntityConverter(new CustomRequestEntityConverter());

		OAuth2AccessTokenResponseHttpMessageConverter tokenResponseHttpMessageConverter = new OAuth2AccessTokenResponseHttpMessageConverter();
		tokenResponseHttpMessageConverter.setAccessTokenResponseConverter(new CustomTokenResponseConverter());

		RestTemplate restTemplate = new RestTemplate(Arrays.asList(new FormHttpMessageConverter(), tokenResponseHttpMessageConverter));
		restTemplate.setErrorHandler(new OAuth2ErrorResponseErrorHandler());
		accessTokenResponseClient.setRestOperations(restTemplate);

		LOGGER.debug(LOGGER_MSG_END);
		return accessTokenResponseClient;
	}

	Converter<Jwt, AbstractAuthenticationToken> authenticationConverter() {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		JwtAuthenticationConverter jwtAuthenticationConverter = new JwtAuthenticationConverter();
		jwtAuthenticationConverter.setJwtGrantedAuthoritiesConverter(new JwtGrantedAuthorityConverter());

		LOGGER.debug(LOGGER_MSG_END);
		return jwtAuthenticationConverter;
	}

	/**
	 * Map authorities from "groups" or "roles" claim in ID Token.
	 *
	 * @return a {@link GrantedAuthoritiesMapper} that maps groups from
	 * the IdP to Spring Security Authorities.
	 */
	@Bean
	public GrantedAuthoritiesMapper userAuthoritiesMapper() {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
		LOGGER.debug(LOGGER_MSG_END);

		return authorities -> {
			LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
			Set<GrantedAuthority> mappedAuthorities = new HashSet<>();

			authorities.forEach(
				authority -> {
					if (authority instanceof OidcUserAuthority) {
						LOGGER.debug("authority instanceof OidcUserAuthority");
						OidcUserAuthority oidcUserAuthority = (OidcUserAuthority) authority;

						if (oidcUserAuthority != null && oidcUserAuthority.getIdToken() != null && oidcUserAuthority.getIdToken().getClaims() != null) {
							LOGGER.debug("oidcUserAuthority.getIdToken().getClaims() NOT NULL");
							LOGGER.debug("oidcUserAuthority.getIdToken().getClaims().getClass()={}", oidcUserAuthority.getIdToken().getClaims().getClass());

							Set<GrantedAuthority> checkedGrantedAuthorities = checkGrantedAuthorities(SecurityUtil.extractAuthorityFromClaims(oidcUserAuthority.getIdToken().getClaims()));
							mappedAuthorities.addAll(checkedGrantedAuthorities);
						}
						else {
							LOGGER.warn("oidcUserAuthority.getIdToken().getClaims() return NULL > Non e' possibile rilevare le GrantedAuthority");
						}

					}
				}
			);
			LOGGER.debug(LOGGER_MSG_END);
			return mappedAuthorities;
		};
	}

	private Set<GrantedAuthority> checkGrantedAuthorities(List<GrantedAuthority> grantedAuthorities) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		Set<GrantedAuthority> mappedAuthorities = new HashSet<>();

		for (GrantedAuthority grantedAuthority : grantedAuthorities) {
			LOGGER.debug("grantedAuthority={}", grantedAuthority);

			try {
				authorityService.getAuthorityFromCode(grantedAuthority.getAuthority());
				mappedAuthorities.add(grantedAuthority);
			}
			catch(AuthorityCodeNotValidException ex) {
				String errorPayload = "Authority code non valido > Authority non riconosciuta [code='" + grantedAuthority.getAuthority() + "']";
				LOGGER.warn(errorPayload);
			}
		}

		if (mappedAuthorities.isEmpty()) {
			String errorPayload = "Authentication is not valid: Authorities is empty";
			LOGGER.warn(errorPayload);
			throw new InsufficientAuthenticationException(errorPayload);
		}

		LOGGER.debug(LOGGER_MSG_END);
		return mappedAuthorities;
	}

	@Bean
	JwtDecoder jwtDecoder(ClientRegistrationRepository clientRegistrationRepository) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		LOGGER.debug("cfgJwkSetUri={}", cfgJwkSetUri);

		NimbusJwtDecoder jwtDecoder = NimbusJwtDecoder.withJwkSetUri(cfgJwkSetUri).build();

		// Create a simple RestTemplate without HttpClient 5
		RestTemplate restTemplate = new RestTemplate();
		jwtDecoder.setClaimSetConverter(
			new CustomClaimConverter(clientRegistrationRepository.findByRegistrationId("oidc"), restTemplate)
		);

		LOGGER.debug(LOGGER_MSG_END);
		return jwtDecoder;
	}

}
