package it.deloitte.postrxade.security.oauth;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.jwt.MappedJwtClaimSetConverter;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;
import org.springframework.security.oauth2.server.resource.web.DefaultBearerTokenResolver;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Claim converter to add custom claims by retrieving the user from the userinfo endpoint.
 */
public class CustomClaimConverter implements Converter<Map<String, Object>, Map<String, Object>> {

	private static final Logger LOGGER = LoggerFactory.getLogger(CustomClaimConverter.class);
	private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
	private static final String LOGGER_MSG_END = "Fine";
	private static final String GROUPS = "groups";

	private final BearerTokenResolver bearerTokenResolver = new DefaultBearerTokenResolver();

	private final MappedJwtClaimSetConverter delegate = MappedJwtClaimSetConverter.withDefaults(Collections.emptyMap());

	private final RestTemplate restTemplate;

	private final ClientRegistration registration;

	private final Map<String, ObjectNode> users = new HashMap<>();

	public CustomClaimConverter(ClientRegistration registration, RestTemplate restTemplate) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
		this.registration = registration;
		this.restTemplate = restTemplate;
		LOGGER.debug(LOGGER_MSG_END);
	}

	public Map<String, Object> convert(Map<String, Object> claims) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		LOGGER.info("Azure ID Token claims received - Keys: {}", claims.keySet());
		LOGGER.info(claims.entrySet().stream()
				.map(entry -> entry.getKey() + "= " + entry.getValue())
				.collect(Collectors.joining(", ")));

		Map<String, Object> convertedClaims = this.delegate.convert(claims);
		if (RequestContextHolder.getRequestAttributes() != null) {
			// Retrieve and set the token

			// String token = bearerTokenResolver.resolve(
			// 	((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest()
			// );

			String token = null;
			ServletRequestAttributes servletRequestAttributes = (ServletRequestAttributes)RequestContextHolder.getRequestAttributes();

			if (servletRequestAttributes != null) {
				LOGGER.debug("servletRequestAttributes != null");
				token = bearerTokenResolver.resolve(servletRequestAttributes.getRequest());
			}
			else {
				LOGGER.warn("servletRequestAttributes = null");
			}

			HttpHeaders headers = new HttpHeaders();
			headers.set("Authorization", buildBearer(token));

			// Retrieve user infos from OAuth provider if not already loaded
			ObjectNode user = users.computeIfAbsent(
				claims.get("sub").toString(),
				s -> {
					ResponseEntity<ObjectNode> userInfo = restTemplate.exchange(
						registration.getProviderDetails().getUserInfoEndpoint().getUri(),
						HttpMethod.GET,
						new HttpEntity<String>(headers),
						ObjectNode.class
					);
					return userInfo.getBody();
				}
			);

			// Add custom claims
			if (user != null) {
				convertedClaims.put("preferred_username", user.get("preferred_username").asText());
				convertedClaims.put("given_name", user.get("given_name").asText());
				convertedClaims.put("family_name", user.get("family_name").asText());
				if (user.has(GROUPS)) {
					List<String> groups = StreamSupport
						.stream(user.get(GROUPS).spliterator(), false)
						.map(JsonNode::asText)
						.collect(Collectors.toList());
					convertedClaims.put(GROUPS, groups);
					LOGGER.info("Azure groups from userinfo: {}", groups);
				}
			}
		}

		LOGGER.debug(LOGGER_MSG_END);
		return convertedClaims;
	}

	private String buildBearer(String token) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
		String result = "Bearer " + token;
		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}
}
