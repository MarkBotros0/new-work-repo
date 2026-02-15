package it.deloitte.postrxade.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum AuthIdProfilo {
    AUDITOR("ADTR", "Auditor"),
    MANAGER("MNGR", "Manager"),
    APPROVER("APRV", "Approver"),
    REVIEWER("RVWR", "Reviewer"),
    SURER("SPR", "Super");

    private final String authCode;
    private final String description;
}
