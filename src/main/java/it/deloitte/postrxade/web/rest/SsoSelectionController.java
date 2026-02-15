package it.deloitte.postrxade.web.rest;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Controller for SSO provider selection page.
 * <p>
 * This controller displays a selection page where users can choose between
 * different SSO providers (Nexi Auth and Microsoft Entra ID).
 */
@Controller
public class SsoSelectionController {

    /**
     * Displays the SSO provider selection page.
     * <p>
     * Endpoint: GET /sso-select
     * <p>
     * This endpoint shows a page with buttons to select the desired SSO provider.
     * When a user clicks on a provider button, they are redirected to the
     * corresponding OAuth2 authorization endpoint handled by Spring Security.
     *
     * @param model The Spring MVC model used to pass attributes to the view template.
     * @return The name of the view to render ("sso-selection").
     */
    @GetMapping("/sso-select")
    public String ssoSelection(Model model) {
        return "sso-selection";
    }
}

