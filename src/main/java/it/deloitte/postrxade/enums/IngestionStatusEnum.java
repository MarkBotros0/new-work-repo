package it.deloitte.postrxade.enums;

import lombok.Getter;

/**
 * Enumeration for Ingestion Statuses.
 * Replaces magic strings like "Failed", "Success".
 */
@Getter
public enum IngestionStatusEnum {

    // Priority used for batch status calculation (higher = more severe)
    FAILED("Failed", 3),
    PROCESSING("Processing", 2),
    SUCCESS("Success", 1),
    UNKNOWN("UNKNOWN", 0);

    private final String label;
    private final int priority;

    IngestionStatusEnum(String label, int priority) {
        this.label = label;
        this.priority = priority;
    }
}
