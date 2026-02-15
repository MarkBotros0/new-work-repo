package it.deloitte.postrxade.records;

public record ErrorRecordCause(
        String description,
        String errorCode
) {
}
