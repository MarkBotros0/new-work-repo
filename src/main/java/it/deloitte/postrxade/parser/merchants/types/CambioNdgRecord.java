package it.deloitte.postrxade.parser.merchants.types;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CambioNdgRecord {
    String intermediario;
    String ndgVecchio;
    String ndgNuovo;
    String filler;
    String controlloDiFineRiga;
}
