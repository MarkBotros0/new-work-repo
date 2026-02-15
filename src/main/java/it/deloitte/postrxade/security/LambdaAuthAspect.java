package it.deloitte.postrxade.security;

import it.deloitte.postrxade.exception.ActionNotPermittedException;
import jakarta.servlet.http.HttpServletRequest;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;


@Aspect
@Component
@org.springframework.context.annotation.Profile("!batch")
public class LambdaAuthAspect {
    @Value("${application.security.lambda-token}")
    private String lambdaToken;

    @Before("@annotation(it.deloitte.postrxade.security.LambdaAuthenticated)")
    public void validateLambdaSecret() throws ActionNotPermittedException {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder
                .currentRequestAttributes()).getRequest();

        String authHeader = request.getHeader("X-Lambda-Auth");

        if (lambdaToken == null || !lambdaToken.equals(authHeader)) {
            throw new ActionNotPermittedException("Invalid or missing Lambda authentication token");
        }
    }
}
