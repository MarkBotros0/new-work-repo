package it.deloitte.postrxade.dto;


public record ValidationPageDTO(
        long totalErrorTransactionCount,
        long totalTransactionCount,
        IssuesGroup rapportiIssues,
        IssuesGroup soggettiIssues,
        IssuesGroup datiContabiliIssues,
        IssuesGroup collegamentiIssues
) {
}

