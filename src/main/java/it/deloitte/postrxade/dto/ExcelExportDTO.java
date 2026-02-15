package it.deloitte.postrxade.dto;

import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExcelExportDTO {

    private List<ExcelRowDTO> errors;
    private List<ExcelRowDTO> warnings;

}