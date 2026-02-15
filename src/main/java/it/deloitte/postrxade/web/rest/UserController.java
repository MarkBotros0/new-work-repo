package it.deloitte.postrxade.web.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for managing user-related requests.
 * <p>
 * This controller provides endpoints to retrieve context about the currently authenticated user.
 * It is primarily used by the frontend to fetch profile details (name, email, etc.)
 * extracted from the Spring Security context or OAuth2 token.
 */
@RestController
@RequestMapping("/api/user")
@CrossOrigin("*") // Allowing cross-origin for development ease, adjust for production
@Tag(name = "User Service", description = "Endpoints for retrieving current user details")
public class UserController {

    /**
     * Retrieves the details of the currently authenticated user.
     * <p>
     * Endpoint: GET /api/user/me
     * <p>
     * Logic:
     * 1. Accesses the {@link SecurityContextHolder} to get the current Authentication object.
     * 2. Validates that the user is actually authenticated and not "anonymous".
     * 3. Extracts user attributes (Name, Email, ID), with specific handling for {@link OAuth2User} principals.
     *
     * @return A {@link ResponseEntity} containing a Map of user profile attributes, or 401 Unauthorized.
     */
    @GetMapping("/me")
    @Operation(summary = "Get current user", description = "Retrieves profile information for the logged-in user")
    public ResponseEntity<Map<String, Object>> getCurrentUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        // 1. Security Check: Ensure user is authenticated and not anonymous
        if (authentication == null || !authentication.isAuthenticated() ||
                "anonymousUser".equals(authentication.getPrincipal())) {
            return ResponseEntity.status(401).build();
        }

        Map<String, Object> userInfo = new HashMap<>();

        // 2. Extract Details based on Principal type
        if (authentication.getPrincipal() instanceof OAuth2User oauth2User) {
            // Mapping for OAuth2/OIDC Providers (e.g., Keycloak, Azure AD)
            userInfo.put("name", oauth2User.getAttribute("name"));
            userInfo.put("email", oauth2User.getAttribute("email"));
            userInfo.put("username", oauth2User.getAttribute("preferred_username"));
            userInfo.put("id", oauth2User.getAttribute("sub"));
        } else {
            // Fallback for standard authentication
            userInfo.put("name", authentication.getName());
            userInfo.put("username", authentication.getName());
        }

        return ResponseEntity.ok(userInfo);
    }
}

