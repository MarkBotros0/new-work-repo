package it.deloitte.postrxade.utils;

import it.deloitte.postrxade.dto.AuthorityDTO;
import it.deloitte.postrxade.dto.UserDTO;
import it.deloitte.postrxade.enums.SubmissionStatusEnum;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static it.deloitte.postrxade.enums.SubmissionStatusEnum.*;

/**
 * Utility Service for handling Workflow Permissions.
 * <p>
 * This class defines the <strong>State Machine Access Control List (ACL)</strong> for the application.
 * It maps every possible status transition (e.g., from "Validation" to "Approval") to the specific
 * user roles (Authorities) allowed to execute it.
 * <p>
 * Constants defined here represent:
 * <ul>
 * <li><strong>Roles:</strong> (e.g., SUPER, IT, APPROVER) matching the database Authority IDs.</li>
 * <li><strong>Status Orders:</strong> (e.g., 2=DATA_VALIDATION, 9=SUBMITTED) matching SubmissionStatus Entity orders.</li>
 * </ul>
 */
@Service
public class PermissionUtil {

    // --- Authority Constants (Must match DB Authority IDs) ---
    private static final String STANDARD = "STND";
    private static final String SUPER = "SPR";
    private static final String IT = "IT";
    private static final String AUDITOR = "ADTR";
    private static final String MANAGER = "MNGR";
    private static final String REVIEWER = "RVWR";
    private static final String APPROVER = "APPR";

    /**
     * The State Machine Configuration.
     * Structure: Map<CurrentStatusID, Map<TargetStatusID, Set<AllowedRoleIDs>>>
     */
    private static final Map<Integer, Map<Integer, Set<String>>> TRANSITION_RULES = new HashMap<>();

    static {
        // --- PHASE 1: Validation & Internal Approval ---
        addRule(VALIDATION_COMPLETED, NEXIS_APPROVAL,       APPROVER, REVIEWER, SUPER);

        addRule(NEXIS_APPROVAL,       PROCESSING,           IT);
        addRule(NEXIS_APPROVAL,       VALIDATION_COMPLETED, APPROVER, STANDARD, SUPER); // Reject back

        // --- PHASE 2: External Reviews ---
        addRule(DELOITTE_REVIEW,      PROCESSING,           SUPER);
        addRule(DELOITTE_REVIEW,      CLIENT_REVIEW,        SUPER);

        addRule(CLIENT_REVIEW,        DELOITTE_REVIEW,      APPROVER, STANDARD, SUPER); // Reject back
        addRule(CLIENT_REVIEW,        PENDING_SUBMISSION,   SUPER);

        // --- PHASE 3: Submission & Finalization ---
        // From Pending
        addRule(PENDING_SUBMISSION,   SUBMITTED,            SUPER);
        addRule(PENDING_SUBMISSION,   CANCELLED,            SUPER);
        addRule(PENDING_SUBMISSION,   REJECTED,             SUPER);

        // From Submitted
        addRule(SUBMITTED,            COMPLETED,            SUPER);
        addRule(SUBMITTED,            CANCELLED,            SUPER);
        addRule(SUBMITTED,            REJECTED,             SUPER);

        // From Completed
        addRule(COMPLETED,            CANCELLED,            SUPER);
    }

    /**
     * Checks if a user has the required authority to move a submission.
     */
    public boolean isPermitted(UserDTO userDetails, Integer destinationStatusOrder, Integer currentStatusOrder) throws NotFoundRecordException {

        if (userDetails == null) return false;

        // 1. Get the Allowed Roles for this specific transition
        Set<String> allowedRoles = getAllowedRoleIds(currentStatusOrder, destinationStatusOrder);

        // 2. Extract User's Roles
        Set<String> userRoles = Optional.ofNullable(userDetails.getAuthorities())
                .orElse(Collections.emptySet())
                .stream()
                .filter(Objects::nonNull)
                .map(AuthorityDTO::getId)
                .collect(Collectors.toSet());

        // 3. Check for intersection
        // Returns true if the user has at least one of the allowed roles
        return allowedRoles.stream().anyMatch(userRoles::contains);
    }

    /**
     * Retrieves the Set of allowed Role IDs for a transition.
     * Uses O(1) Map lookup.
     */
    private Set<String> getAllowedRoleIds(Integer currentId, Integer destId) throws NotFoundRecordException {

        if (TRANSITION_RULES.containsKey(currentId)) {
            Map<Integer, Set<String>> targets = TRANSITION_RULES.get(currentId);

            if (targets.containsKey(destId)) {
                return targets.get(destId);
            }
        }

        throw new NotFoundRecordException("No valid permission rule defined for transition: " + currentId + " -> " + destId);
    }

    /**
     * Helper to populate the static map.
     */
    private static void addRule(SubmissionStatusEnum current, SubmissionStatusEnum target, String... roles) {
        TRANSITION_RULES
                .computeIfAbsent(current.getOrder(), k -> new HashMap<>())
                .put(target.getOrder(), Set.of(roles));
    }
}
