package it.deloitte.postrxade.security.oauth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.RequestEntity;
import org.springframework.security.oauth2.client.endpoint.OAuth2AuthorizationCodeGrantRequest;
import org.springframework.security.oauth2.client.endpoint.OAuth2AuthorizationCodeGrantRequestEntityConverter;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

public class CustomRequestEntityConverter implements Converter<OAuth2AuthorizationCodeGrantRequest, RequestEntity<?>> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CustomRequestEntityConverter.class);
	private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
	private static final String LOGGER_MSG_END = "Fine";

	// oauth

	private OAuth2AuthorizationCodeGrantRequestEntityConverter defaultConverter;

	public CustomRequestEntityConverter() {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
		defaultConverter = new OAuth2AuthorizationCodeGrantRequestEntityConverter();
		LOGGER.debug(LOGGER_MSG_END);
	}

	@Override
	@SuppressWarnings("unchecked")
	public RequestEntity<?> convert(OAuth2AuthorizationCodeGrantRequest req) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		RequestEntity<?> entity = defaultConverter.convert(req);

		RequestEntity<?> result = null;

		if (entity != null) {
			MultiValueMap<String, String> params = (MultiValueMap<String, String>) entity.getBody();

			if (params != null) {
//				TODO make logger debug mode
				LOGGER.info("params.entrySet()={}", params.entrySet());
				// Log all parameter keys to verify client_secret is present
				LOGGER.info("params.keySet()={}", params.keySet());
				
				// If client_secret is not present, add it manually
				if (!params.containsKey("client_secret")) {
					LOGGER.warn("client_secret is NOT present in params! Adding it manually...");
					
					ClientRegistration clientRegistration = req.getClientRegistration();
					String clientSecret = clientRegistration.getClientSecret();
					
					if (clientSecret != null && !clientSecret.isEmpty()) {
						// Create a new MultiValueMap to avoid modifying the original
						MultiValueMap<String, String> newParams = new LinkedMultiValueMap<>(params);
						newParams.add("client_secret", clientSecret);
						params = newParams;
						LOGGER.debug("client_secret added successfully");
					} else {
						LOGGER.error("client_secret is null or empty in ClientRegistration!");
					}
				} else {
					LOGGER.debug("client_secret is present in params");
				}
			}

			result = new RequestEntity<>(params, entity.getHeaders(), entity.getMethod(), entity.getUrl());
		}

		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

}