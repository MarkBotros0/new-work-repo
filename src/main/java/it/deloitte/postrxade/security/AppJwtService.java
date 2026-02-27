package it.deloitte.postrxade.security;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import com.nimbusds.jose.proc.SecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Servizio per la creazione di JWT emessi dall'applicazione dopo il login SSO.
 * Il token viene inviato al frontend nel redirect post-autenticazione e usato
 * come Bearer token per le chiamate API (evitando dipendenza da cookie cross-origin).
 * Non caricato in profilo batch/output (nessun login, nessun JWT).
 */
@Service
@Profile("!batch & !output")
public class AppJwtService {

	private static final Logger LOGGER = LoggerFactory.getLogger(AppJwtService.class);
	private static final String CLAIM_GROUPS = "groups";

	private final JwtEncoder jwtEncoder;
	private final long expirationSeconds;

	public AppJwtService(
			@Value("${application.security.app-jwt.secret}") String secretBase64,
			@Value("${application.security.app-jwt.expiration-seconds:3600}") long expirationSeconds
	) {
		this.expirationSeconds = expirationSeconds;
		byte[] secretBytes = Base64.getDecoder().decode(secretBase64);
		if (secretBytes.length < 32) {
			throw new IllegalArgumentException("application.security.app-jwt.secret must be at least 256 bits (32 bytes) when base64-decoded");
		}
		SecretKey key = new SecretKeySpec(secretBytes, "HmacSHA256");
		ImmutableSecret<SecurityContext> jwkSource = new ImmutableSecret<>(key);
		this.jwtEncoder = new NimbusJwtEncoder(jwkSource);
		LOGGER.info("AppJwtService initialized with expiration {} seconds", expirationSeconds);
	}

	/**
	 * Crea un JWT contenente identità e authorities dell'utente autenticato (OIDC/OAuth2).
	 * Il frontend riceverà questo token nel redirect e lo invierà come Authorization: Bearer.
	 * 
	 * @param authentication l'autenticazione dell'utente
	 * @param tenantId il tenant ID (opzionale, per multi-tenant)
	 * @return il token JWT come stringa
	 */
	public String createToken(Authentication authentication, String tenantId) {
		Map<String, Object> attributes;
		List<String> authorities;

		if (authentication.getPrincipal() instanceof OidcUser oidcUser) {
			attributes = oidcUser.getAttributes();
			authorities = authentication.getAuthorities().stream()
					.map(GrantedAuthority::getAuthority)
					.collect(Collectors.toList());
		} else if (authentication.getPrincipal() instanceof OAuth2User oauth2User) {
			attributes = oauth2User.getAttributes();
			authorities = authentication.getAuthorities().stream()
					.map(GrantedAuthority::getAuthority)
					.collect(Collectors.toList());
		} else {
			throw new IllegalArgumentException("Authentication principal must be OidcUser or OAuth2User");
		}

		Instant now = Instant.now();
		JwtClaimsSet.Builder claimsBuilder = JwtClaimsSet.builder()
				.claim(JwtClaimNames.SUB, attributes.get("sub"))
				.claim(JwtClaimNames.IAT, now)
				.expiresAt(now.plusSeconds(expirationSeconds))
				.claim(CLAIM_GROUPS, authorities);

		if (attributes.containsKey("preferred_username")) {
			claimsBuilder.claim("preferred_username", attributes.get("preferred_username"));
		}
		if (attributes.containsKey("name")) {
			claimsBuilder.claim("name", attributes.get("name"));
		}
		if (attributes.containsKey("given_name")) {
			claimsBuilder.claim("given_name", attributes.get("given_name"));
		}
		if (attributes.containsKey("family_name")) {
			claimsBuilder.claim("family_name", attributes.get("family_name"));
		}
		if (attributes.containsKey("email")) {
			claimsBuilder.claim("email", attributes.get("email"));
		}
		if (attributes.containsKey("mail")) {
			claimsBuilder.claim("mail", attributes.get("mail"));
		}
		if (attributes.containsKey("company")) {
			claimsBuilder.claim("company", attributes.get("company"));
		}
		if (attributes.containsKey("department")) {
			claimsBuilder.claim("department", attributes.get("department"));
		}
		
		// Include tenant_id in the JWT for multi-tenant support
		if (tenantId != null && !tenantId.isBlank()) {
			claimsBuilder.claim("tenant_id", tenantId);
		}

		JwtClaimsSet claims = claimsBuilder.build();
		// Specificare HS256 nell'header così NimbusJwtEncoder seleziona la SecretKey (HMAC) invece di cercare RSA
		JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).build();
		String token = jwtEncoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
		LOGGER.debug("Created app JWT for sub={}", attributes.get("sub"));
		return token;
	}
}
