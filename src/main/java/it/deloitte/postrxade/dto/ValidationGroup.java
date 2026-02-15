package it.deloitte.postrxade.dto;

import java.util.List;

/**
 * A validation group (e.g., 'error' or 'warning').
 * (Corresponds to the 'ValidationGroup' interface)
 *
 * @param count The total count of items in this group.
 * @param reasons A list of specific reasons and their individual counts.
 */
public record ValidationGroup(
        long count,
        List<ValidationReason> reasons
) {}
