package it.deloitte.postrxade.records;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public record TransactionDateCount(String dtOpe, Long count) {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("ddMMyyyy");

    public LocalDate date() {
        return LocalDate.parse(dtOpe, FORMATTER);
    }
}
