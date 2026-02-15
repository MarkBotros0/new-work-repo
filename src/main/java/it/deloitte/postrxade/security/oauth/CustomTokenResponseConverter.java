package it.deloitte.postrxade.security.oauth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.endpoint.OAuth2AccessTokenResponse;
import org.springframework.security.oauth2.core.endpoint.OAuth2ParameterNames;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CustomTokenResponseConverter implements Converter<Map<String, Object>, OAuth2AccessTokenResponse> {
	private static final Logger LOGGER = LoggerFactory.getLogger(CustomTokenResponseConverter.class);
	private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
	private static final String LOGGER_MSG_END = "Fine";

	private static final Set<String> TOKEN_RESPONSE_PARAMETER_NAMES = Stream.of(
		OAuth2ParameterNames.ACCESS_TOKEN,
		OAuth2ParameterNames.TOKEN_TYPE,
		OAuth2ParameterNames.EXPIRES_IN,
		OAuth2ParameterNames.REFRESH_TOKEN,
		OAuth2ParameterNames.SCOPE
	).collect(Collectors.toSet());

	@Override
	public OAuth2AccessTokenResponse convert(Map<String, Object> tokenResponseParameters) {
		LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

		tokenResponseParameters.forEach((key, value) -> LOGGER.debug("key={}, value={}", key, value));

		String accessToken = (String)tokenResponseParameters.get(OAuth2ParameterNames.ACCESS_TOKEN);
		LOGGER.debug("accessToken={}", accessToken);

		OAuth2AccessToken.TokenType accessTokenType = null;
		if (tokenResponseParameters.containsKey(OAuth2ParameterNames.TOKEN_TYPE) &&
			tokenResponseParameters.get(OAuth2ParameterNames.TOKEN_TYPE) != null &&
			tokenResponseParameters.get(OAuth2ParameterNames.TOKEN_TYPE) instanceof String &&
			OAuth2AccessToken.TokenType.BEARER.getValue().equalsIgnoreCase((String)tokenResponseParameters.get(OAuth2ParameterNames.TOKEN_TYPE))
		) {
			accessTokenType = OAuth2AccessToken.TokenType.BEARER;
		}

		if (accessTokenType != null) {
			LOGGER.debug("accessTokenType={}", accessTokenType.getValue());
		}
		else {
			LOGGER.debug("accessTokenType is NULL");
		}

		long expiresIn = 0;
		if (tokenResponseParameters.containsKey(OAuth2ParameterNames.EXPIRES_IN) &&
			tokenResponseParameters.get(OAuth2ParameterNames.EXPIRES_IN) != null &&
			tokenResponseParameters.get(OAuth2ParameterNames.EXPIRES_IN) instanceof Integer
		) {
			try {
				expiresIn = (Integer)tokenResponseParameters.get(OAuth2ParameterNames.EXPIRES_IN);
			}
			catch (NumberFormatException ex) {
				LOGGER.warn("Lettura EXPIRES_IN non riuscita. Messaggio={}", ex.getMessage());
			}
		}
		LOGGER.debug("expiresIn={}", expiresIn);

		Set<String> scopes = Collections.emptySet();
		if (tokenResponseParameters.containsKey(OAuth2ParameterNames.SCOPE) &&
			tokenResponseParameters.get(OAuth2ParameterNames.SCOPE) != null &&
			tokenResponseParameters.get(OAuth2ParameterNames.SCOPE) instanceof String
		) {
			String scope = (String)tokenResponseParameters.get(OAuth2ParameterNames.SCOPE);
			scopes = Arrays.stream(StringUtils.delimitedListToStringArray(scope, " ")).collect(Collectors.toSet());
		}
		LOGGER.debug("scopes={}", scopes);

		String refreshToken = null;
		if (tokenResponseParameters.containsKey(OAuth2ParameterNames.REFRESH_TOKEN) &&
			tokenResponseParameters.get(OAuth2ParameterNames.REFRESH_TOKEN) != null &&
			tokenResponseParameters.get(OAuth2ParameterNames.REFRESH_TOKEN) instanceof String
		) {
			refreshToken = (String)tokenResponseParameters.get(OAuth2ParameterNames.REFRESH_TOKEN);
		}
		LOGGER.debug("refreshToken={}", refreshToken);

		Map<String, Object> additionalParameters = new LinkedHashMap<>();
		tokenResponseParameters.entrySet()
			.stream()
			.filter(e -> !TOKEN_RESPONSE_PARAMETER_NAMES.contains(e.getKey()))
			.forEach(e -> additionalParameters.put(e.getKey(), e.getValue()));

		OAuth2AccessTokenResponse result = OAuth2AccessTokenResponse.withToken(accessToken)
			.tokenType(accessTokenType)
			.expiresIn(expiresIn)
			.scopes(scopes)
			.refreshToken(refreshToken)
			.additionalParameters(additionalParameters)
			.build();

		LOGGER.debug(LOGGER_MSG_END);
		return result;
	}

}
