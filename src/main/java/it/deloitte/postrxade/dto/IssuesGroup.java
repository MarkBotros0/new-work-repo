package it.deloitte.postrxade.dto;


public record IssuesGroup(
        ValidationGroup errorValidationGroup,
        ValidationGroup warningValidationGroup
) {}
