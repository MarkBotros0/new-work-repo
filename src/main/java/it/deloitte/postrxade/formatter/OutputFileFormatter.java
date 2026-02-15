package it.deloitte.postrxade.formatter;

import it.deloitte.postrxade.entity.*;
import lombok.NoArgsConstructor;

import java.time.format.DateTimeFormatter;

@NoArgsConstructor
public final class OutputFileFormatter {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ddMMyyyy");
    private static String os = System.getProperty("os.name").toLowerCase();

    /**
     * Header (Type 0 record)
     * Format: 398 characters + end-of-line
     */
    public static String createHeader() {
        StringBuilder sb = new StringBuilder(398);

        // 1. Tipo record (position 1, length 1): always "0"
        sb.append("0");

        // 2. Codice fornitura (positions 2-6, length 5): always "ARU00"
        sb.append("ARU00");

        // 3. Tipologia invio (position 7, length 1)
        // 1=Ordinario, 2=Straordinario, 3=Annullamento
        sb.append("1");

        // 4. Tipologia comunicazione (position 8, length 1)
        // 1=Nuovi rapporti, 2=Aggiornamento, 3=Saldi annuali, 4=Cancellazione,
        // 5=Chiusura rapporti, 6=Reimpianto, 7=Cambio ID, 8=Presa in carico,
        // 9=Annullamento file, C=Chiusura generalizzata
        sb.append("1");

        // 5. Anno riferimento (positions 9-12, length 4)
        // Mandatory for tipo comunicazione 1, 3, 5; "0000" otherwise
        sb.append("0000");

        // 6. Mese riferimento (positions 13-14, length 2)
        // Mandatory for tipo comunicazione 1, 5; "00" otherwise
        sb.append("00");

        // 7. Protocollo da annullare (positions 15-38, length 24)
        // Only for tipo comunicazione 9 (Annullamento file)
        sb.append(" ".repeat(24));

        // 8. Codice fiscale (positions 39-54, length 16)
        sb.append(rightPad("04107060966", 16, ' '));

        // 9-14. Fields for persona fisica (spaces for persona giuridica)
        sb.append(" ".repeat(26));  // 9.  Cognome (positions 55-80)
        sb.append(" ".repeat(25));  // 10. Nome (positions 81-105)
        sb.append(" ");             // 11. Sesso (position 106)
        sb.append(" ".repeat(8));   // 12. Data nascita (positions 107-114)
        sb.append(" ".repeat(40));  // 13. Comune nascita (positions 115-154)
        sb.append(" ".repeat(2));   // 14. Provincia nascita (positions 155-156)

        // 15-17. Fields for persona giuridica
        // 15. Denominazione (positions 157-226, length 70)
        sb.append(rightPad("Nexi Payments SpA", 70, ' '));

        // 16. Comune sede legale (positions 227-266, length 40)
        sb.append(rightPad("Milano", 40, ' '));

        // 17. Provincia sede legale (positions 267-268, length 2)
        sb.append("03");

        // 18. CF società incorporata (positions 269-284, length 16)
        // Only for tipo comunicazione 8 (Presa in carico)
        sb.append(" ".repeat(16));

        // 19. Flag cessazione (position 285, length 1)
        // 0=default, 1=cessazione attività (only for tipo comunicazione 3)
        sb.append("0");

        // 20. Data chiusura generalizzata (positions 286-293, length 8)
        // Only for tipo comunicazione C (Chiusura generalizzata), format DDMMYYYY
        sb.append(" ".repeat(8));

        // 21. Filler (positions 294-397, length 104)
        sb.append(" ".repeat(104));

        // 22. Carattere controllo (position 398, length 1): always "A"
        sb.append("A");

        // 23. End of line
        sb.append(getEndOfLine());

        return sb.toString();
    }


    /**
     * Footer (Type 9 record)
     * Format: 398 characters + end-of-line
     */
    public static String createFooter(int numTipo1, int numTipo2, int numTipo3) {
        StringBuilder sb = new StringBuilder(398);

        // 1. Tipo record (position 1, length 1): always "9"
        sb.append("9");

        // 2. Numero record tipo 1 (positions 2-10, length 9)
        // Count of Type 1 records in the file
        sb.append(String.format("%09d", numTipo1));

        // 3. Numero record tipo 2 (positions 11-19, length 9)
        // Count of Type 2 records in the file
        sb.append(String.format("%09d", numTipo2));

        // 4. Numero record tipo 3 (positions 20-28, length 9)
        // Count of Type 3 records in the file
        sb.append(String.format("%09d", numTipo3));

        // 5. Numero record tipo 4 (positions 29-37, length 9)
        // Always "000000000" (Type 4 records not used)
        sb.append("000000000");

        // 6. Filler (positions 38-397, length 360)
        sb.append(" ".repeat(360));

        // 7. Carattere di controllo (position 398, length 1): always "A"
        sb.append("A");

        // 8. End of line
        sb.append(getEndOfLine());

        return sb.toString();
    }

    /**
     * Section 1 (Type 1): Collegamenti + Rapporti
     * Format: 398 characters + end-of-line
     */
    public static String toRapportiOutputString(Collegamenti collegamenti) {
        StringBuilder sb = new StringBuilder(398);
        
        Rapporti rapporti = collegamenti.getRapporto();

        // 1. Tipo record (position 1, length 1): always "1"
        sb.append("1");

        // 2. Codice univoco rapporto (positions 2-51, length 50)
        // This is the unique identifier - using chiave_rapporto from Collegamenti
        sb.append(rightPad(nullSafe(collegamenti.getChiaveRapporto()), 50, ' '));

        if (rapporti != null) {
            // 3. Tipo rapporto (positions 52-53, length 2)
            sb.append(rightPad(nullSafe(rapporti.getTipoRapportoInterno()), 2, ' '));

            // 4. Descrizione rapporto (positions 54-77, length 24)
            // Only if tipo rapporto = "99", otherwise spaces
            String tipoRapporto = nullSafe(rapporti.getTipoRapportoInterno()).trim();
            if ("99".equals(tipoRapporto)) {
                sb.append(rightPad(nullSafe(rapporti.getNote()), 24, ' '));
            } else {
                sb.append(" ".repeat(24));
            }

            // 5. Data inizio rapporto (positions 78-85, length 8, format DDMMYYYY)
            String dataInizio = nullSafe(rapporti.getDataInizioRapporto()).trim();
            if (dataInizio.isEmpty()) {
                sb.append("01010001");
            } else {
                // Convert from YYYYMMDD to DDMMYYYY
                sb.append(convertDateFormat(dataInizio));
            }

            // 6. Data fine rapporto (positions 86-93, length 8, format DDMMYYYY)
            String dataFine = nullSafe(rapporti.getDataFineRapporto()).trim();
            if (dataFine.isEmpty()) {
                sb.append("01010001");
            } else {
                // Convert from YYYYMMDD to DDMMYYYY
                sb.append(convertDateFormat(dataFine));
            }

            // 7. CAB (positions 94-98, length 5)
            // Mandatory for tipo rapporto 1, 2, 3, 12, 13, 96, 97
            sb.append(rightPad(nullSafe(rapporti.getCab()), 5, ' '));
        } else {
            // Default values when Rapporti is null
            sb.append(" ".repeat(2));   // tipo_rapporto
            sb.append(" ".repeat(24));  // descrizione
            sb.append("01010001");      // data_inizio
            sb.append("01010001");      // data_fine
            sb.append(" ".repeat(5));   // cab
        }

        // 8. Identificativo esito (positions 99-111, length 13)
        // Used only for extraordinary submissions, spaces otherwise
        sb.append(" ".repeat(13));

        // 9. Filler (positions 112-397, length 286)
        sb.append(" ".repeat(286));

        // 10. Carattere di controllo (position 398): always "A"
        sb.append("A");

        // 11. End of line (CRLF for Windows, LF for Unix/Linux)
        sb.append(getEndOfLine());

        return sb.toString();
    }
    
    /**
     * Convert date from YYYYMMDD to DDMMYYYY format
     * If input is already in DDMMYYYY format or invalid, return as-is (padded to 8 chars)
     */
    private static String convertDateFormat(String date) {
        if (date == null || date.trim().isEmpty()) {
            return "01010001";
        }
        
        date = date.trim();
        
        // If already 8 characters and starts with day (01-31), assume it's already DDMMYYYY
        if (date.length() == 8) {
            String firstTwo = date.substring(0, 2);
            try {
                int day = Integer.parseInt(firstTwo);
                if (day >= 1 && day <= 31) {
                    // Likely already in DDMMYYYY format
                    return rightPad(date, 8, ' ');
                }
            } catch (NumberFormatException e) {
                // Not a valid number, return as-is
                return rightPad(date, 8, ' ');
            }
            
            // Try to convert from YYYYMMDD to DDMMYYYY
            try {
                String year = date.substring(0, 4);
                String month = date.substring(4, 6);
                String day = date.substring(6, 8);
                return day + month + year;
            } catch (Exception e) {
                return rightPad(date, 8, ' ');
            }
        }
        
        return rightPad(date, 8, ' ');
    }


    /**
     * Section 2 (Type 2): Collegamenti + Rapporti + Soggetti
     * Format: 398 characters + end-of-line
     */
    public static String toCollegamentiOutputString(Collegamenti collegamenti) {
        StringBuilder sb = new StringBuilder(398);

        Rapporti rapporto = collegamenti.getRapporto();
        Soggetti soggetto = collegamenti.getSoggetto();

        // 1. Tipo record (position 1, length 1): always "2"
        sb.append("2");

        // 2. Codice univoco rapporto (positions 2-51, length 50)
        sb.append(rightPad(nullSafe(collegamenti.getChiaveRapporto()), 50, ' '));

        // 3. Progressivo anagrafica (positions 52-60, length 9)
        // Progressive counter starting from "000000001"
        // TODO: This should be calculated based on the order within the submission
        sb.append("000000001");

        if (rapporto != null) {
            // 4. Data inizio partecipazione (positions 61-68, length 8, format DDMMYYYY)
            String dataInizio = nullSafe(rapporto.getDataInizioRapporto()).trim();
            if (dataInizio.isEmpty()) {
                sb.append("01010001");
            } else {
                sb.append(convertDateFormat(dataInizio));
            }

            // 5. Data fine partecipazione (positions 69-76, length 8, format DDMMYYYY)
            String dataFine = nullSafe(rapporto.getDataFineRapporto()).trim();
            if (dataFine.isEmpty()) {
                sb.append("01010001");
            } else {
                sb.append(convertDateFormat(dataFine));
            }
        } else {
            sb.append("01010001");  // data_inizio default
            sb.append("01010001");  // data_fine default
        }

        if (soggetto != null) {
            String codiceFiscale = nullSafe(soggetto.getCodiceFiscale()).trim();
            
            // 6. Flag assenza codice fiscale (position 77, length 1)
            if (codiceFiscale.isEmpty()) {
                sb.append("1");  // Codice fiscale assente
                // 7. Codice fiscale (positions 78-93, length 16) - spaces when absent
                sb.append(" ".repeat(16));
            } else {
                sb.append("0");  // Codice fiscale presente
                // 7. Codice fiscale (positions 78-93, length 16)
                sb.append(rightPad(codiceFiscale, 16, ' '));
            }

            // 8. Codice ruolo (position 94, length 1)
            // Convert Nexi codes to AdE codes
            String ruolo = convertRuoloCode(nullSafe(collegamenti.getRuolo()));
            sb.append(ruolo);

            // Check if person is fisica (F) or giuridica (G)
            String tipoSoggetto = nullSafe(soggetto.getTipoSoggetto()).trim();
            boolean isPersonaFisica = "F".equalsIgnoreCase(tipoSoggetto);

            if (isPersonaFisica) {
                // Fields 9-14: For persona fisica
                // 9. Cognome (positions 95-120, length 26)
                sb.append(rightPad(nullSafe(soggetto.getCognome()), 26, ' '));

                // 10. Nome (positions 121-145, length 25)
                sb.append(rightPad(nullSafe(soggetto.getNome()), 25, ' '));

                // 11. Sesso (position 146, length 1)
                sb.append(rightPad(nullSafe(soggetto.getSesso()), 1, ' '));

                // 12. Data di nascita (positions 147-154, length 8, format DDMMYYYY)
                String dataNascita = nullSafe(soggetto.getDataNascita()).trim();
                if (dataNascita.isEmpty()) {
                    sb.append(" ".repeat(8));
                } else {
                    sb.append(convertDateFormat(dataNascita));
                }

                // 13. Comune di nascita (positions 155-194, length 40)
                sb.append(rightPad(nullSafe(soggetto.getComune()), 40, ' '));

                // 14. Provincia di nascita (positions 195-196, length 2)
                sb.append(rightPad(nullSafe(soggetto.getProvincia()), 2, ' '));

                // Fields 15-17: Spaces for persona fisica
                sb.append(" ".repeat(60));  // Denominazione
                sb.append(" ".repeat(40));  // Comune sede legale
                sb.append(" ".repeat(2));   // Provincia sede legale
            } else {
                // Fields 9-14: Spaces for persona giuridica
                sb.append(" ".repeat(26));  // Cognome
                sb.append(" ".repeat(25));  // Nome
                sb.append(" ");             // Sesso
                sb.append(" ".repeat(8));   // Data nascita
                sb.append(" ".repeat(40));  // Comune nascita
                sb.append(" ".repeat(2));   // Provincia nascita

                // Fields 15-17: For persona giuridica
                // 15. Denominazione (positions 197-256, length 60)
                // Merge cognome + nome for denominazione
                String denominazione = (nullSafe(soggetto.getCognome()) + " " + nullSafe(soggetto.getNome())).trim();
                sb.append(rightPad(denominazione, 60, ' '));

                // 16. Comune sede legale (positions 257-296, length 40)
                sb.append(rightPad(nullSafe(soggetto.getComune()), 40, ' '));

                // 17. Provincia sede legale (positions 297-298, length 2)
                sb.append(rightPad(nullSafe(soggetto.getProvincia()), 2, ' '));
            }
        } else {
            // Default values when Soggetto is null
            sb.append("1");              // flag_assenza_cf (absent)
            sb.append(" ".repeat(16));   // codice_fiscale
            sb.append("0");              // ruolo (default to titolare)
            sb.append(" ".repeat(26));   // cognome
            sb.append(" ".repeat(25));   // nome
            sb.append(" ");              // sesso
            sb.append(" ".repeat(8));    // data_nascita
            sb.append(" ".repeat(40));   // comune_nascita
            sb.append(" ".repeat(2));    // provincia_nascita
            sb.append(" ".repeat(60));   // denominazione
            sb.append(" ".repeat(40));   // comune_sede
            sb.append(" ".repeat(2));    // provincia_sede
        }

        // 18. Filler (positions 299-397, length 99)
        sb.append(" ".repeat(99));

        // 19. Carattere di controllo (position 398): always "A"
        sb.append("A");

        // 20. End of line
        sb.append(getEndOfLine());

        return sb.toString();
    }

    /**
     * Convert Nexi role codes to AdE role codes
     * T/C -> 0 (Titolare/Cointestatario)
     * I -> 4 (Titolare ditta individuale)
     * D -> 5 (Delegato/Procuratore)
     * O -> 6 (Delegato sportello/occasionale)
     * E -> 7 (Titolare effettivo)
     * G -> 8 (Garantito)
     * A -> 9 (Agente mono/plurimandatario)
     */
    private static String convertRuoloCode(String nexiCode) {
        if (nexiCode == null || nexiCode.trim().isEmpty()) {
            return "0";  // Default to titolare
        }
        
        return switch (nexiCode.trim().toUpperCase()) {
            case "T", "C" -> "0";  // Titolare/Cointestatario
            case "I" -> "4";       // Titolare ditta individuale
            case "D" -> "5";       // Delegato/Procuratore
            case "O" -> "6";       // Delegato sportello/occasionale
            case "E" -> "7";       // Titolare effettivo
            case "G" -> "8";       // Garantito
            case "A" -> "9";       // Agente mono/plurimandatario
            default -> "0";        // Default to titolare
        };
    }

    /**
     * Section 3 (Type 3): Collegamenti + Rapporti + DatiContabili
     * Format: 398 characters + end-of-line
     */
    public static String toCollegamentiOutputString2(Collegamenti collegamenti) {
        StringBuilder sb = new StringBuilder(398);

        Rapporti rapporto = collegamenti.getRapporto();
        DatiContabili datiContabili = collegamenti.getDatiContabili();

        // 1. Tipo record (position 1, length 1): always "3"
        sb.append("3");

        // 2. Codice univoco rapporto (positions 2-51, length 50)
        sb.append(rightPad(nullSafe(collegamenti.getChiaveRapporto()), 50, ' '));

        if (rapporto != null) {
            // 3. Tipo rapporto (positions 52-53, length 2)
            sb.append(rightPad(nullSafe(rapporto.getTipoRapportoInterno()), 2, ' '));
        } else {
            sb.append(" ".repeat(2));
        }

        if (datiContabili != null) {
            // 4. Anno di riferimento (positions 54-57, length 4)
            sb.append(rightPad(nullSafe(datiContabili.getAnnoDiRiferimento()), 4, ' '));

            // 5. Importo 1 - Saldo finale (positions 58-74, length 17)
            // Nexi sends 18 digits, need to cut the second digit from left (first indicates sign)
            sb.append(formatImporto(datiContabili.getImportoSaldoFinale(), 17));

            // 6. Flag soglia Importo 1 (position 75, length 1)
            sb.append(rightPad(nullSafe(datiContabili.getFlagSogliaSaldoFinale()), 1, ' '));

            // 7. Importo 2 - Saldo iniziale (positions 76-92, length 17)
            sb.append(formatImporto(datiContabili.getImportoSaldoIniziale(), 17));

            // 8. Flag soglia Importo 2 (position 93, length 1)
            sb.append(rightPad(nullSafe(datiContabili.getFlagSogliaSaldoIniziale()), 1, ' '));

            // 9. Importo 3 - Totale operazioni attive (positions 94-110, length 17)
            sb.append(formatImporto(datiContabili.getTotaleOperazioniAttive(), 17));

            // 10. Flag soglia Importo 3 (position 111, length 1)
            sb.append(rightPad(nullSafe(datiContabili.getFlagSogliaOperazioniAttive()), 1, ' '));

            // 11. Importo 4 - Totale operazioni passive (positions 112-128, length 17)
            sb.append(formatImporto(datiContabili.getTotaleOperazioniPassive(), 17));

            // 12. Flag soglia Importo 4 (position 129, length 1)
            sb.append(rightPad(nullSafe(datiContabili.getFlagSogliaOperazioniPassive()), 1, ' '));

            // 13. Altre informazioni (positions 130-146, length 17)
            sb.append(formatImporto(datiContabili.getAltreInformazioni(), 17));

            // 14. Flag soglia Altre informazioni (position 147, length 1)
            // Fixed value "0" according to specification
            sb.append("0");

            // 15. Giacenza media (positions 148-164, length 17)
            sb.append(formatImporto(datiContabili.getGiacenzaMedia(), 17));

            // 16. Flag soglia Giacenza media (position 165, length 1)
            sb.append(rightPad(nullSafe(datiContabili.getFlagSogliaGiacenzaMedia()), 1, ' '));

            // 17. Valuta UIF (positions 166-168, length 3)
            sb.append(rightPad(nullSafe(datiContabili.getDivisa()), 3, ' '));
        } else {
            // Default values when DatiContabili is null
            sb.append(" ".repeat(4));   // anno_di_riferimento
            sb.append(" ".repeat(17));  // importo_saldo_finale
            sb.append(" ");             // flag_soglia_saldo_finale
            sb.append(" ".repeat(17));  // importo_saldo_iniziale
            sb.append(" ");             // flag_soglia_saldo_iniziale
            sb.append(" ".repeat(17));  // totale_operazioni_attive
            sb.append(" ");             // flag_soglia_operazioni_attive
            sb.append(" ".repeat(17));  // totale_operazioni_passive
            sb.append(" ");             // flag_soglia_operazioni_passive
            sb.append(" ".repeat(17));  // altre_informazioni
            sb.append("0");             // flag_soglia_altre_informazioni (fixed)
            sb.append(" ".repeat(17));  // giacenza_media
            sb.append(" ");             // flag_soglia_giacenza_media
            sb.append(" ".repeat(3));   // divisa
        }

        // 18. Natura valuta (positions 169-170, length 2)
        // Valorize with one space according to specification
        sb.append(" ");

        // 19. Identificativo esito (positions 171-183, length 13)
        // Used only for tipo comunicazione 3, spaces otherwise
        sb.append(" ".repeat(13));

        // 20. Importo 5 (positions 184-200, length 17)
        // For year 2023+ only, spaces otherwise
        sb.append(" ".repeat(17));

        // 21. Flag soglia Importo 5 (position 201, length 1)
        sb.append("0");

        // 22. Filler (positions 202-397, length 196)
        sb.append(" ".repeat(196));

        // 23. Carattere di controllo (position 398): always "A"
        sb.append("A");

        // 24. End of line
        sb.append(getEndOfLine());

        return sb.toString();
    }

    /**
     * Format importo field: Nexi sends 18 digits, need to cut the second digit from left
     * The first digit indicates the sign, so we keep it and skip the second digit
     * Example: +1234567890123456 (18 chars) -> +123456789012345 (17 chars)
     */
    private static String formatImporto(String importo, int targetLength) {
        if (importo == null || importo.trim().isEmpty()) {
            return " ".repeat(targetLength);
        }
        
        importo = importo.trim();
        
        // If the importo is 18 characters, cut the second digit
        if (importo.length() == 18) {
            // Keep first char (sign) + skip second char + keep rest
            String sign = importo.substring(0, 1);
            String rest = importo.substring(2);  // Skip second character
            importo = sign + rest;
        }
        
        // Pad or truncate to target length
        return rightPad(importo, targetLength, ' ');
    }

    public static String toDatiContabiliOutputString(DatiContabili datiContabili) {
        StringBuilder sb = new StringBuilder();

        return sb.toString();
    }

    public static String toOutputFileString(ResolvedTransaction transaction) {
        Merchant merchant = transaction.getMerchant();
        StringBuilder sb = new StringBuilder();

        String tipoRecord = transaction.getTpRec();
        sb.append(tipoRecord);

        String codiceFiscale = "04107060966";
        sb.append(rightPad(codiceFiscale, 16, ' '));

        sb.append(transaction.getTipoOpe());

        sb.append(transaction.getDtOpe().toString());

        String totOpeStr = transaction.getTotOpe().toString();
        sb.append(rightPad(totOpeStr, 9, '0'));

        String impOpe = Integer.toString(transaction.getImpOpe().intValue());
        sb.append(rightPad(impOpe, 9, '0'));

        String currency = transaction.getDivisaOpe();
        sb.append(currency);

        sb.append(rightPad(nullSafe(transaction.getIdEsercente()), 30, ' '));

        sb.append(rightPad(nullSafe(transaction.getIdPos()), 30, ' '));

        sb.append(transaction.getTipoPag());

        String codFiscaleEsercente = merchant.getCodFiscale();
        sb.append(rightPad(codFiscaleEsercente, 16, ' '));

        String partitaIvaEsercente = merchant.getPartitaIva();
        sb.append(rightPad(partitaIvaEsercente, 11, ' '));

        String idSalmov = merchant.getIdSalmov();
        sb.append(rightPad(idSalmov, 50, ' '));

        sb.append(" ".repeat(160));

        sb.append('A');

        sb.append(getEndOfLine());

        return sb.toString();
    }

    private static String nullSafe(String value) {
        return value != null ? value : "";
    }

    private static String leftPad(String value, int length, char padChar) {
        if (value == null) {
            value = "";
        }
        if (value.length() >= length) {
            return value.substring(0, length);
        }
        StringBuilder sb = new StringBuilder(length);
        for (int i = value.length(); i < length; i++) {
            sb.append(padChar);
        }
        sb.append(value);
        return sb.toString();
    }

    public static String getEndOfLine() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            return "\r\n";
        } else {
            return "\n";
        }
    }

    private static String rightPad(String value, int length, char padChar) {
        if (value == null) {
            value = "";
        }
        if (value.length() >= length) {
            return value.substring(0, length);
        }
        StringBuilder sb = new StringBuilder(length);
        sb.append(value);
        for (int i = value.length(); i < length; i++) {
            sb.append(padChar);
        }
        return sb.toString();
    }
}


