package it.deloitte.postrxade.security.oauth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.keygen.Base64StringKeyGenerator;
import org.springframework.security.crypto.keygen.StringKeyGenerator;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.DefaultOAuth2AuthorizationRequestResolver;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestResolver;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;
import org.springframework.security.oauth2.core.endpoint.PkceParameterNames;

import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class CustomAuthorizationRequestResolver implements OAuth2AuthorizationRequestResolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(CustomAuthorizationRequestResolver.class);
	private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
	private static final String LOGGER_MSG_END = "Fine";

	// oauth

	private final StringKeyGenerator secureKeyGenerator = new Base64StringKeyGenerator(Base64.getUrlEncoder().withoutPadding(), 96);

	private final OAuth2AuthorizationRequestResolver defaultAuthorizationRequestResolver;

	public CustomAuthorizationRequestResolver(ClientRegistrationRepository clientRegistrationRepository) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
		// this.defaultAuthorizationRequestResolver = new DefaultOAuth2AuthorizationRequestResolver(clientRegistrationRepository, "/oauth2/authorization");
		this.defaultAuthorizationRequestResolver = new DefaultOAuth2AuthorizationRequestResolver(clientRegistrationRepository, "/api/authorization");
		LOGGER.debug(LOGGER_MSG_END);
	}

	@Override
	public OAuth2AuthorizationRequest resolve(HttpServletRequest request) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		OAuth2AuthorizationRequest authorizationRequest = this.defaultAuthorizationRequestResolver.resolve(request);

		// return authorizationRequest != null ?
		// 		customAuthorizationRequest(authorizationRequest) :
		// 		null;

		OAuth2AuthorizationRequest result = null;

		if (authorizationRequest != null) {
			result = customAuthorizationRequest(authorizationRequest);
		}
		else {
			LOGGER.debug("authorizationRequest is NULL");
		}

		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	@Override
	public OAuth2AuthorizationRequest resolve(HttpServletRequest request, String clientRegistrationId) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
		LOGGER.debug("clientRegistrationId={}", clientRegistrationId);

		OAuth2AuthorizationRequest authorizationRequest = this.defaultAuthorizationRequestResolver.resolve(request, clientRegistrationId);

		OAuth2AuthorizationRequest result = null;

		if (authorizationRequest != null) {
			result = customAuthorizationRequest(authorizationRequest);
		}
		else {
			LOGGER.debug("authorizationRequest is NULL");
		}

		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	private OAuth2AuthorizationRequest customAuthorizationRequest(OAuth2AuthorizationRequest req) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		OAuth2AuthorizationRequest result = null;

		if (req != null) {
			Map<String, Object> attributes = new HashMap<>(req.getAttributes());
			Map<String, Object> additionalParameters = new HashMap<>(req.getAdditionalParameters());
			addPkceParameters(attributes, additionalParameters);
			result = OAuth2AuthorizationRequest.from(req)
				.attributes(attributes)
				.additionalParameters(additionalParameters)
				.build();
		}

		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

	private void addPkceParameters(Map<String, Object> attributes, Map<String, Object> additionalParameters) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		String codeVerifier = this.secureKeyGenerator.generateKey();
		attributes.put(PkceParameterNames.CODE_VERIFIER, codeVerifier);
		try {
			String codeChallenge = createHash(codeVerifier);
			additionalParameters.put(PkceParameterNames.CODE_CHALLENGE, codeChallenge);
			additionalParameters.put(PkceParameterNames.CODE_CHALLENGE_METHOD, "S256");
		}
		catch (NoSuchAlgorithmException e) {
			additionalParameters.put(PkceParameterNames.CODE_CHALLENGE, codeVerifier);
		}

		LOGGER.debug(LOGGER_MSG_END);
	}

	private static String createHash(String value) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		byte[] digest = md.digest(value.getBytes(StandardCharsets.US_ASCII));
		return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
	}

}
