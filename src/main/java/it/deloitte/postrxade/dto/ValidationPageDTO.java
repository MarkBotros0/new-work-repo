package it.deloitte.postrxade.dto;


public record ValidationPageDTO(
        long totalTransactionCount,
        IssuesGroup rapportiIssues,
        IssuesGroup soggettiIssues,
        IssuesGroup collegamentiIssues,
        IssuesGroup datiContabiliIssues
) {
}

