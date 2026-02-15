package it.deloitte.postrxade.web.rest;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Controller for serving the application entry point (Home Page).
 * <p>
 * Unlike the API controllers, this class uses {@link Controller} to serve an HTML view ("index").
 * It interacts with the Spring Security context to inject authentication state directly into the
 * initial page load, allowing the frontend to render appropriate UI elements (like the user's name)
 * without needing an immediate secondary API call.
 */
@Controller
public class HomeController {

    /**
     * Handles the root URL request and prepares the "index" view.
     * <p>
     * Endpoint: GET /
     * <p>
     * Logic:
     * 1. Retrieves the current {@link Authentication} from the security context.
     * 2. Determines if the user is fully authenticated (filtering out "anonymousUser").
     * 3. Populates the {@link Model} with:
     * - {@code isAuthenticated}: Boolean flag.
     * - {@code userName}: The display name (or email) if the user is an OAuth2 principal.
     *
     * @param model The Spring MVC model used to pass attributes to the view template.
     * @return The name of the view to render ("index").
     */
    @GetMapping("/")
    public String home(Model model) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        // Check if user is logged in (and not just an anonymous token)
        boolean isAuthenticated = authentication != null && 
                                 authentication.isAuthenticated() && 
                                 !"anonymousUser".equals(authentication.getPrincipal());
        
        model.addAttribute("isAuthenticated", isAuthenticated);

        // If logged in via OAuth2, extract user details for the UI greeting
        if (isAuthenticated && authentication.getPrincipal() instanceof OAuth2User oauth2User) {
            String userName = oauth2User.getAttribute("name");
            String userEmail = oauth2User.getAttribute("email");

            // Prefer name, fallback to email
            model.addAttribute("userName", userName != null ? userName : userEmail);
        }
        
        return "index";
    }
}
