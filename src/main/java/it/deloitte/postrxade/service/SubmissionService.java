package it.deloitte.postrxade.service;

import it.deloitte.postrxade.dto.SubmissionStatusChangeDTO;
import it.deloitte.postrxade.dto.SubmissionStatusGroupDTO;
import it.deloitte.postrxade.dto.UserDTO;
import it.deloitte.postrxade.entity.Obligation;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.entity.SubmissionStatus;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.exception.UserNotValidException;
import it.deloitte.postrxade.utils.PermissionUtil;

import java.io.IOException;
import java.util.List;

/**
 * Service interface for managing Submission workflows.
 * <p>
 * This service handles the lifecycle of a Submission, specifically focusing on
 * Status transitions (State Machine) and retrieving available status options for the UI.
 */
public interface SubmissionService {

    /**
     * Low-level method to update a submission's status in the persistence layer.
     * <p>
     * This method is typically called internally after all business rules and
     * permission checks have been validated.
     *
     * @param submission                  The submission entity to update.
     * @param submissionStatusDestination The target status entity.
     * @param submitter                   The user performing the action (for audit/logging).
     */
    void changeStatus(Submission submission, SubmissionStatus submissionStatusDestination, UserDTO submitter) throws IOException, NotFoundRecordException;

    /**
     * High-level entry point for changing a submission's status.
     * <p>
     * This method orchestrates the transition:
     * 1. Validates the existence of the submission and target status.
     * 2. Checks if the current user has permission (via {@link PermissionUtil}) to perform the transition.
     * 3. Calls {@link #changeStatus} if valid.
     *
     * @param submissionStatusChangeDTO DTO containing submission ID, target status, and user context.
     * @throws UserNotValidException   If the user does not have the required authority.
     * @throws NotFoundRecordException If the submission or status is not found.
     */
    void performChangeStatusAction(SubmissionStatusChangeDTO submissionStatusChangeDTO) throws UserNotValidException, NotFoundRecordException, IOException;

    /**
     * Retrieves all available status options, grouped logically for the UI.
     * <p>
     * Used to populate filter dropdowns or status selection lists.
     *
     * @return A list of {@link SubmissionStatusGroupDTO}.
     */
    List<SubmissionStatusGroupDTO> getStatusOptions();

    Submission createSubmissionByObligation(Obligation obligation) throws NotFoundRecordException;

    void markAsError(Submission submission);

    /**
     * Updates submission status without user context (for internal/system use).
     * Used during automated ingestion process.
     *
     * @param submission The submission to update
     * @param statusOrder The order of the target status (1=INGESTION_FINISHED, 2=DATA_VALIDATION, 3=VALIDATION_COMPLETED, etc.)
     * @throws NotFoundRecordException If the status is not found
     */
    void updateStatusInternal(Submission submission, Integer statusOrder) throws NotFoundRecordException;

    Submission getActivSubmission(Obligation obligation);
}

