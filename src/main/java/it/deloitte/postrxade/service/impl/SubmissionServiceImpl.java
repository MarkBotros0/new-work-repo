package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.dto.SubmissionStatusChangeDTO;
import it.deloitte.postrxade.dto.SubmissionStatusGroupDTO;
import it.deloitte.postrxade.dto.UserDTO;
import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.enums.SubmissionStatusEnum;
import it.deloitte.postrxade.exception.InvalidStatusTransitionException;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.exception.ResourceNotFoundException;
import it.deloitte.postrxade.exception.UserNotValidException;
import it.deloitte.postrxade.repository.SubmissionRepository;
import it.deloitte.postrxade.repository.SubmissionStatusGroupRepository;
import it.deloitte.postrxade.repository.SubmissionStatusRepository;
import it.deloitte.postrxade.service.OutputService;
import it.deloitte.postrxade.service.SubmissionService;
import it.deloitte.postrxade.service.UserService;
import it.deloitte.postrxade.utils.AuditLogger;
import it.deloitte.postrxade.utils.PermissionUtil;
import lombok.extern.slf4j.Slf4j;
import ma.glasnost.orika.MapperFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

@Slf4j
@Service
public class SubmissionServiceImpl implements SubmissionService {


    private final AuditLogger appLogger;

    private final UserService userService;

    private final OutputService outputService;

    private final PermissionUtil permissionUtil;

    private SubmissionRepository submissionRepository;

    private SubmissionStatusRepository submissionStatusRepository;

    private final SubmissionStatusGroupRepository submissionStatusGroupRepository;

    @Autowired
    private @Qualifier("alternativeMapperFacade") MapperFacade alternativeMapperFacade;


    public SubmissionServiceImpl(UserService userService, PermissionUtil permissionUtil, SubmissionRepository submissionRepository,
                                 SubmissionStatusGroupRepository submissionStatusGroupRepository, SubmissionRepository submissionTypeRepository, AuditLogger appLogger, OutputService outputService) {
        this.userService = userService;
        this.permissionUtil = permissionUtil;
        this.submissionRepository = submissionRepository;
        this.submissionStatusGroupRepository = submissionStatusGroupRepository;
        this.appLogger = appLogger;
        this.outputService = outputService;
    }

    @Autowired
    public void setSubmissionRepository(SubmissionRepository submissionRepository, SubmissionStatusRepository submissionStatusRepository) {
        this.submissionStatusRepository = submissionStatusRepository;
        this.submissionRepository = submissionRepository;
    }

    public void changeStatus(Submission submission, SubmissionStatus submissionStatusDestination, UserDTO userDTO) throws IOException, NotFoundRecordException {

        SubmissionStatus beforeSubmissionStatus = submission.getCurrentSubmissionStatus();

        if (!(Objects.equals(submissionStatusDestination.getOrder(), SubmissionStatusEnum.CANCELLED.getOrder())) && !(Objects.equals(submissionStatusDestination.getOrder(), SubmissionStatusEnum.REJECTED.getOrder()))) { // if we want to cancel the submission don't validate the step
            validateSubmissionOrder(submission, submissionStatusDestination);
        } else {
            submission.setLastSubmissionStatus(submission.getCurrentSubmissionStatus());
            submission.setCancelledAt(LocalDateTime.now());

        }
        submission.setCurrentSubmissionStatus(submissionStatusDestination);
        submission.setLastUpdateBy(alternativeMapperFacade.map(userDTO, User.class));
        submissionRepository.save(submission);

        // log it
        appLogger.save(Log.builder()
                .message(String.format(AuditLogger.STATUS_CHANGE,
                        userDTO.getFirstName(),
                        userDTO.getLastName(),
                        submission.getId(),
                        submission.getObligation().getFiscalYear(),
                        submission.getObligation().getPeriod().getName(),
                        beforeSubmissionStatus.getName(),
                        submissionStatusDestination.getName()))
                .timestamp(Instant.now())
                .submission(submission)
                .updater(alternativeMapperFacade.map(userDTO, User.class))
                .beforeSubmissionStatus(beforeSubmissionStatus)
                .afterSubmissionStatus(submissionStatusDestination)
                .build());
    }

    private void validateSubmissionOrder(Submission submission, SubmissionStatus submissionStatusDestination) {
        int diff = Math.abs(submission.getCurrentSubmissionStatus().getOrder()
                - submissionStatusDestination.getOrder());

        if (diff > 1) {
            throw new InvalidStatusTransitionException(
                    "Cannot skip statuses: current=" + submission.getCurrentSubmissionStatus().getName() +
                            ", attempted=" + submissionStatusDestination.getName()
            );
        }

    }

    @Override
    @Transactional
    public void performChangeStatusAction(SubmissionStatusChangeDTO submissionStatusChangeDTO)
            throws UserNotValidException, NotFoundRecordException, IOException {

        String funcIdentifier = "[UPD_SUB_STATUS-" + submissionStatusChangeDTO.getSubmissionId() + "]";
        log.info("{} Starting performChangeStatusAction for destinationStatusOrder={}", funcIdentifier, submissionStatusChangeDTO.getDestinationStatusOrder());

        UserDTO userDetails = userService.getCurrentUser();
        log.debug("{} Retrieved current user: id={}", funcIdentifier, userDetails.getId());

        Submission submission = submissionRepository.findOneById(submissionStatusChangeDTO.getSubmissionId())
                .orElseThrow(() -> {
                    log.warn("{} Submission not found for id={}", funcIdentifier, submissionStatusChangeDTO.getSubmissionId());
                    return new NotFoundRecordException("Submission not found with id: " + submissionStatusChangeDTO.getSubmissionId());
                });

        log.debug("{} Found submission with currentStatusOrder={}", funcIdentifier, submission.getCurrentSubmissionStatus().getOrder());

        SubmissionStatus destinationSubmissionStatus = submissionStatusRepository.findOneByOrder(submissionStatusChangeDTO.getDestinationStatusOrder())
                .orElseThrow(() -> {
                    log.warn("{} Destination status not found for order={}", funcIdentifier, submissionStatusChangeDTO.getDestinationStatusOrder());
                    return new NotFoundRecordException("Submission Status not found with order: " + submissionStatusChangeDTO.getDestinationStatusOrder());
                });

        log.debug("{} Destination status resolved successfully: {}", funcIdentifier, destinationSubmissionStatus.getName());

        Integer currentStatusOrder = submission.getCurrentSubmissionStatus().getOrder();

        if (!permissionUtil.isPermitted(userDetails, submissionStatusChangeDTO.getDestinationStatusOrder(), currentStatusOrder)) {
            log.warn("{} Permission denied for userId={} to change status from {} -> {}",
                    funcIdentifier, userDetails.getId(), currentStatusOrder, submissionStatusChangeDTO.getDestinationStatusOrder());
            throw new UserNotValidException("User does not have permission for this status change.");
        }

        log.info("{} Attempting status change: from {} to {}", funcIdentifier, currentStatusOrder, submissionStatusChangeDTO.getDestinationStatusOrder());
        changeStatus(submission, destinationSubmissionStatus, userDetails);
        log.info("{} Status change completed successfully for submissionId={}", funcIdentifier, submissionStatusChangeDTO.getSubmissionId());

        if (SubmissionStatusEnum.isOutputRequired(submission.getCurrentSubmissionStatus().getOrder())) {
            outputService.generateSubmissionOutputTxtAsync(submission.getId());
        }
    }


    /**
     * Gets ALL groups and their statuses.
     */
    @Override
    @Transactional(readOnly = true)
    public List<SubmissionStatusGroupDTO> getStatusOptions() {

        List<SubmissionStatusGroup> groups = submissionStatusGroupRepository.findAll();

        return alternativeMapperFacade.mapAsList(groups, SubmissionStatusGroupDTO.class);
    }

    @Override
    public Submission createSubmissionByObligation(Obligation obligation) throws NotFoundRecordException {
        Submission submission = new Submission();
        submission.setObligation(obligation);
        submission.setIsManual(Boolean.TRUE);
        submission.setLastUpdatedAt(LocalDateTime.now());

        // Usa INGESTION_FINISHED (order 1) come status iniziale per le nuove submission
        // La submission passerà a DATA_VALIDATION durante il processamento e poi a VALIDATION_COMPLETED alla fine
        SubmissionStatus submissionStatus = submissionStatusRepository.findOneByOrder(1)
                .orElseThrow(() -> new NotFoundRecordException(
                        "SubmissionStatus with order 1 (INGESTION_FINISHED) not found. " +
                                "Please ensure the database is properly initialized with submission statuses."));

        submission.setCurrentSubmissionStatus(submissionStatus);
        return submissionRepository.save(submission);
    }

    @Override
    @Transactional
    public void markAsError(Submission submission) {
//        error status
        SubmissionStatus submissionStatus = submissionStatusRepository.findOneByOrder(13)
                .orElseThrow(() -> new ResourceNotFoundException(" Status with order 13 is not found"));
        submissionRepository.updateStatus(submission.getId(), submissionStatus);
    }

    /**
     * Updates submission status without user context (for internal/system use).
     * Used during automated ingestion process.
     *
     * @param submission  The submission to update
     * @param statusOrder The order of the target status (1=INGESTION_FINISHED, 2=DATA_VALIDATION, 3=VALIDATION_COMPLETED, etc.)
     * @throws NotFoundRecordException If the status is not found
     */
    @Transactional
    public void updateStatusInternal(Submission submission, Integer statusOrder) throws NotFoundRecordException {
        SubmissionStatus submissionStatus = submissionStatusRepository.findOneByOrder(statusOrder)
                .orElseThrow(() -> new NotFoundRecordException(
                        "SubmissionStatus with order " + statusOrder + " not found. " +
                                "Please ensure the database is properly initialized with submission statuses."));
        submissionRepository.updateStatus(submission.getId(), submissionStatus);
        log.info("Updated submission {} status to order {} ({})",
                submission.getId(), statusOrder, submissionStatus.getName());
    }

    @Override
    public Submission getActivSubmission(Obligation obligation) {
        return submissionRepository
                .findByObligationId(obligation.getId())
                .stream()
                .filter(s -> {
                    return !s.getCurrentSubmissionStatus().getName().equals(SubmissionStatusEnum.ERROR.getDbName())
                            && !s.getCurrentSubmissionStatus().getName().equals(SubmissionStatusEnum.REJECTED.getDbName())
                            && !s.getCurrentSubmissionStatus().getName().equals(SubmissionStatusEnum.CANCELLED.getDbName());
                }).findFirst()
                .orElse(null);
    }
}
