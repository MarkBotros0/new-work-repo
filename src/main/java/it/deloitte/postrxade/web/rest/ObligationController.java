package it.deloitte.postrxade.web.rest;

import java.io.IOException;
import java.util.List;

import it.deloitte.postrxade.dto.*;
import it.deloitte.postrxade.enums.AuthIdProfilo;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.exception.UserNotValidException;
import it.deloitte.postrxade.security.RequireAuthorities;
import it.deloitte.postrxade.service.ObligationService;
import it.deloitte.postrxade.service.SubmissionService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * REST controller for managing Obligations and Submission workflows.
 * <p>
 * This controller handles:
 * <ul>
 * <li>Retrieving lists of submissions based on fiscal periods.</li>
 * <li>Fetching available submission status options.</li>
 * <li>Processing status changes (workflow transitions) for submissions.</li>
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/api/obligation")
@Tag(name = "Obligation Service", description = "Endpoints for managing Obligations and Submission workflows")
public class ObligationController {

    private final ObligationService obligationService;

    private final SubmissionService submissionService;

    /**
     * Constructor-based dependency injection.
     *
     * @param obligationService Service for retrieving obligation data.
     * @param submissionService Service for handling submission logic and status changes.
     */
    public ObligationController(ObligationService obligationService, SubmissionService submissionService) {
        this.obligationService = obligationService;
        this.submissionService = submissionService;
    }

    /**
     * Retrieves the list of submissions for a specific Fiscal Year and Period.
     * <p>
     * Endpoint: GET /api/obligation/submissions
     *
     * @param fy     The Fiscal Year (e.g., 2024).
     * @param period The specific month name (e.g., "January").
     * @return A ResponseEntity containing a list of {@link SubmissionCustomDTO}.
     * @throws NotFoundRecordException if no records are found matching the criteria.
     */
    @GetMapping("/submissions")
    @Operation(summary = "Get the list of submissions", description = "Request to get all submissions by given FY and Period")
    @RequireAuthorities({AuthIdProfilo.REVIEWER, AuthIdProfilo.APPROVER})
    public ResponseEntity<List<SubmissionCustomDTO>> getSubmissionsByFyAndPeriod(
            @RequestParam("fy") Integer fy,
            @RequestParam("period") String period) throws NotFoundRecordException {

        List<SubmissionCustomDTO> customList = obligationService.getAllObligationsByFyAndPeriod(fy, period);
        return new ResponseEntity<>(customList, HttpStatus.OK);
    }

    /**
     * Retrieves all available status options for submissions.
     * <p>
     * Endpoint: GET /api/obligation/status/options
     * <p>
     * Used for populating filter dropdowns or status selection lists in the UI.
     *
     * @return A ResponseEntity containing a list of {@link SubmissionStatusGroupDTO}.
     */
    @GetMapping("/status/options")
    @Operation(summary = "Get status options", description = "Retrieve all available submission status groups/options")
    @RequireAuthorities({AuthIdProfilo.REVIEWER, AuthIdProfilo.APPROVER})
    public ResponseEntity<List<SubmissionStatusGroupDTO>> getStatuses() {
        List<SubmissionStatusGroupDTO> options = submissionService.getStatusOptions();
        return ResponseEntity.ok(options);
    }

    /**
     * Changes the status of a submission (Workflow Transition).
     * <p>
     * Endpoint: POST /api/obligation/status
     *
     * @param submissionStatusChangeDTO The DTO containing the submission ID and the target status action.
     * @return A success message string.
     * @throws UserNotValidException   if the current user is not authorized to perform this action.
     * @throws NotFoundRecordException if the submission or status cannot be found.
     */
    @PostMapping("/status")
    @Operation(summary = "Change submission status", description = "Perform a workflow action to change the status of a submission")
//    @RequireAuthorities({AuthIdProfilo.APPROVER, AuthIdProfilo.SURER})
    public ResponseEntity<String> changeStatus(@RequestBody SubmissionStatusChangeDTO submissionStatusChangeDTO)
            throws UserNotValidException, NotFoundRecordException, IOException {

        submissionService.performChangeStatusAction(submissionStatusChangeDTO);
        return ResponseEntity.ok("Submission status changed successfully.");
    }

    @PostMapping(path = "/ingestions")
    public ResponseEntity<String> ingestFiles() throws IOException, NotFoundRecordException {
        log.debug("Received request to start file ingestion");
        obligationService.ingestObligationFilesAsync();
        log.debug("Successfully initiated async file ingestion");
        return ResponseEntity.accepted().body("Ingestion started");
    }
}