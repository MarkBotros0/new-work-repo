package it.deloitte.postrxade.dto;

/**
 * DTO for aggregated error type counts from database queries.
 * Used to avoid loading all ErrorCause entities into memory.
 */
public record ErrorTypeCountDTO(
        Long errorTypeId,
        String errorTypeName,
        String errorCode,
        Long count
) {}
