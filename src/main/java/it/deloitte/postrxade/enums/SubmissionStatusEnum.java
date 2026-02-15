package it.deloitte.postrxade.enums;

import lombok.Getter;

import java.util.Objects;

/**
 * Enumeration representing the possible states of a Submission workflow.
 * <p>
 * Replaces hardcoded Integers in the codebase.
 */
@Getter
public enum SubmissionStatusEnum {

    INGESTION_FINISHED(1, "INGESTION_FINISHED"),
    DATA_VALIDATION(2, "DATA_VALIDATION"),
    VALIDATION_COMPLETED(3, "VALIDATION_COMPLETED"),
    NEXIS_APPROVAL(4, "NEXIS_APPROVAL"),
    PROCESSING(5, "PROCESSING"),
    DELOITTE_REVIEW(6, "DELOITTE_REVIEW"),
    CLIENT_REVIEW(7, "CLIENT_REVIEW"),
    PENDING_SUBMISSION(8, "PENDING SUBMISSION"), // Note: Keeping space to match legacy DB string if needed
    SUBMITTED(9, "SUBMITTED"),
    COMPLETED(10, "COMPLETED"),
    CANCELLED(11, "CANCELLED"),
    REJECTED(12, "REJECTED"),
    ERROR(13, "ERROR");

    private final Integer order;
    private final String dbName;

    SubmissionStatusEnum(Integer order, String dbName) {
        this.order = order;
        this.dbName = dbName;
    }

    public static SubmissionStatusEnum fromId(Integer id) {
        for (SubmissionStatusEnum status : values()) {
            if (status.order.equals(id)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown SubmissionStatus ID: " + id);
    }

    public static boolean isOutputRequired(Integer order) {
        return Objects.equals(order, NEXIS_APPROVAL.order);
    }
}
