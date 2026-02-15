package it.deloitte.postrxade.dto;

/**
 * A specific validation reason and its occurrence count.
 * (Corresponds to the 'ValidationReason' interface)
 *
 * @param message The validation message.
 * @param count The number of times this specific validation message occurred.
 */
public record ValidationReason(
        String message,
        String errorCode,
        Long count
) {}
