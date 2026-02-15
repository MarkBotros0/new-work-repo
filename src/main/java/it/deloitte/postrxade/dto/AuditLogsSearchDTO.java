package it.deloitte.postrxade.dto;

import it.deloitte.postrxade.utils.SortItem;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@Data
@RequiredArgsConstructor
public class AuditLogsSearchDTO {


    private String fiscalYear;

    private String period;

    @NotNull(message = "page cannot be null")
    @PositiveOrZero(message = "page must be a zero or a positive number")
    private Integer page;


    @NotNull(message = "size cannot be null")
    @Positive(message = "size must be a positive number")
    private Integer size;

    private  List<SortItem> sortList;
}
