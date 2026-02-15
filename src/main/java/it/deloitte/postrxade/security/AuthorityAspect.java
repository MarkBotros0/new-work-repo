package it.deloitte.postrxade.security;

import it.deloitte.postrxade.dto.UserDTO;
import it.deloitte.postrxade.enums.AuthIdProfilo;
import it.deloitte.postrxade.service.AuthorityService;
import it.deloitte.postrxade.service.UserService;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Aspect
@Component
@org.springframework.context.annotation.Profile("!batch")
public class AuthorityAspect {

    @Autowired
    private UserService userService;
    @Autowired
    private AuthorityService authorityService;

    @Before("@annotation(requireAuth)")
    public void validate(RequireAuthorities requireAuth) throws Exception {
        UserDTO user = userService.getCurrentUser();

        Set<String> requiredCodes = Arrays.stream(requireAuth.value())
                .map(AuthIdProfilo::getAuthCode)
                .collect(Collectors.toSet());

        authorityService.checkUserAuthority(user.getId(), requiredCodes);
    }
}
