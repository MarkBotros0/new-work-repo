package it.deloitte.postrxade.security.oauth;

import it.deloitte.postrxade.security.SecurityUtil;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class JwtGrantedAuthorityConverter implements Converter<Jwt, Collection<GrantedAuthority>> {

	public JwtGrantedAuthorityConverter() {
		// Bean extracting authority.
	}

	@Override
	public Collection<GrantedAuthority> convert(Jwt jwt) {
		return SecurityUtil.extractAuthorityFromClaims(jwt.getClaims());
	}

}
