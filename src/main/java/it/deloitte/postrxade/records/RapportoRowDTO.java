package it.deloitte.postrxade.records;

public record RapportoRowDTO(
        String chiaveRapporto,
        String ndg,
        String ruolo,
        String codiceFiscale,
        String cognome,
        String nome,
        String dataInizioRapporto,
        String dataFineRapporto
) {}
