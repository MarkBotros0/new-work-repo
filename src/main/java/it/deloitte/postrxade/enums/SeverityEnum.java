package it.deloitte.postrxade.enums;

import it.deloitte.postrxade.entity.ErrorRecord;
import lombok.Getter;

/**
 * Enumeration for Validation Severity Levels.
 * <p>
 * Centralizes the logic for distinguishing Errors from Warnings.
 * Replaces the "Magic Number" check (level > 1) found across services.
 */
@Getter
public enum SeverityEnum {

    ERROR(2),
    WARNING(1);

    private final int level;

    SeverityEnum(int level) {
        this.level = level;
    }

    /**
     * Determines if a raw severity level represents a Warning.
     * <p>
     * Logic: If the level is equals to WARNING (1) is considered a Warning.
     */
    public static boolean isWarning(SeverityEnum severity) {
        return severity == WARNING;
    }

    public static boolean isWarning(ErrorRecord errorRecord) {
        return errorRecord.getErrorCauses()
                .stream()
                .anyMatch(c -> c.getErrorType().getSeverityLevel() == SeverityEnum.WARNING.getLevel());
    }

    /**
     * Determines if a raw severity level represents an Error.
     * <p>
     * Logic: If the level is equals to ERROR (2) is considered an Error.
     */
    public static boolean isError(Integer level) {
        return level != null && level == ERROR.level;
    }
}