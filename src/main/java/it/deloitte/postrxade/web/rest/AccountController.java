package it.deloitte.postrxade.web.rest;
import java.security.Principal;

import it.deloitte.postrxade.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import it.deloitte.postrxade.dto.UserDTO;
import it.deloitte.postrxade.exception.ResourceException;
import it.deloitte.postrxade.exception.UserNotValidException;

import it.deloitte.postrxade.service.UserService;

/**
 * REST controller for managing the current user's account.
 */
@RestController
@RequestMapping("/api/account")
@Tag(name = "Account Service", description = "All the services involving the user Account")
public class AccountController {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccountController.class);
    private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
    private static final String LOGGER_MSG_END = "Fine";

    private final UserService userService;

    public AccountController(UserService userService) {
        this.userService = userService;
    }

    /**
     * {@code GET /account} : get the current user.
     *
     * @param principal the current user; resolves to {@code null} if not authenticated.
     * @return the current user.
     * @throws ResourceException {@code 500 (Internal Server Error)} if the user couldn't be returned.
     */
    @GetMapping
    @PreAuthorize("isAuthenticated()")
    @Operation(summary = "Get the current user account", description = "Request to get account information based on the current user." )
    //@SuppressWarnings("unchecked")
    public UserDTO getAccount(Principal principal) {
        LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());

        UserDTO result = null;

        if (principal instanceof AbstractAuthenticationToken) {
            LOGGER.debug("Object principal is instance of AbstractAuthenticationToken");
            try {
                result = userService.getUserFromAuthentication((AbstractAuthenticationToken) principal);
            }
            catch (UserNotValidException ex) {
                String errorPayload = "User not valid - Message: " + ex.getMessage();
                LOGGER.error(errorPayload);
                throw new ResourceException("User not valid", ex);
            }
        }
        else {
            LOGGER.debug("Object principal is NOT instance of AbstractAuthenticationToken > Call to [throw new ResourceException(\"User could not be found\")]");
            throw new ResourceException("User could not be found");
        }

        LOGGER.debug(LOGGER_MSG_END);
        return result;
    }

}
