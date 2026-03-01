package it.deloitte.postrxade.formatter;

import it.deloitte.postrxade.entity.*;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
public final class OutputFileFormatter {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ddMMyyyy");
    private static String os = System.getProperty("os.name").toLowerCase();


        // HashMap statica per memorizzare la corrispondenza tra alpha-3 e codice numerico
        private static final Map<String, String> CountryCodeMap = new HashMap<>();

        static {
            // Popolamento iniziale della mappa basato sullo standard ISO 3166
            // A
            CountryCodeMap.put("AFG", "004"); // Afghanistan
            CountryCodeMap.put("ALB", "008"); // Albania
            CountryCodeMap.put("DZA", "012"); // Algeria
            CountryCodeMap.put("AND", "020"); // Andorra
            CountryCodeMap.put("AGO", "024"); // Angola
            CountryCodeMap.put("ΑΙΑ", "660"); // Anguilla
            CountryCodeMap.put("ATA", "010"); // Antartide
            CountryCodeMap.put("ATG", "028"); // Antigua e Barbuda
            CountryCodeMap.put("ANT", "530"); // Antille Olandesi
            CountryCodeMap.put("SAU", "682"); // Arabia Saudita
            CountryCodeMap.put("ARG", "032"); // Argentina
            CountryCodeMap.put("ARM", "051"); // Armenia
            CountryCodeMap.put("ABW", "533"); // Aruba
            CountryCodeMap.put("AUS", "036"); // Australia
            CountryCodeMap.put("AUT", "040"); // Austria
            CountryCodeMap.put("AZE", "031"); // Azerbaijan

            // B
            CountryCodeMap.put("BHS", "044"); // Bahamas
            CountryCodeMap.put("BHR", "048"); // Bahrain
            CountryCodeMap.put("BGD", "050"); // Bangladesh
            CountryCodeMap.put("BRB", "052"); // Barbados
            CountryCodeMap.put("BEL", "056"); // Belgio
            CountryCodeMap.put("BLZ", "084"); // Belize
            CountryCodeMap.put("BEN", "204"); // Benin
            CountryCodeMap.put("BMU", "060"); // Bermuda
            CountryCodeMap.put("BLR", "112"); // Bielorussia
            CountryCodeMap.put("BTN", "064"); // Bhutan
            CountryCodeMap.put("BOL", "068"); // Bolivia
            CountryCodeMap.put("BIH", "070"); // Bosnia Erzegovina
            CountryCodeMap.put("BWA", "072"); // Botswana
            CountryCodeMap.put("BRA", "076"); // Brasile
            CountryCodeMap.put("BRN", "096"); // Brunei Darussalam
            CountryCodeMap.put("BGR", "100"); // Bulgaria
            CountryCodeMap.put("BFA", "854"); // Burkina Faso
            CountryCodeMap.put("BDI", "108"); // Burundi

            // C
            CountryCodeMap.put("KHM", "116"); // Cambogia
            CountryCodeMap.put("CMR", "120"); // Camerun
            CountryCodeMap.put("CAN", "124"); // Canada
            CountryCodeMap.put("CPV", "132"); // Capo Verde
            CountryCodeMap.put("TCD", "148"); // Ciad
            CountryCodeMap.put("CHL", "152"); // Cile
            CountryCodeMap.put("CHN", "156"); // Cina
            CountryCodeMap.put("CYP", "196"); // Cipro
            CountryCodeMap.put("VAT", "336"); // Città del Vaticano
            CountryCodeMap.put("COL", "170"); // Colombia
            CountryCodeMap.put("COM", "174"); // Comore
            CountryCodeMap.put("PRK", "408"); // Corea del Nord
            CountryCodeMap.put("KOR", "410"); // Corea del Sud
            CountryCodeMap.put("CRI", "188"); // Costa Rica
            CountryCodeMap.put("CIV", "384"); // Costa d'Avorio
            CountryCodeMap.put("HRV", "191"); // Croazia
            CountryCodeMap.put("CUB", "192"); // Cuba

            // D-E
            CountryCodeMap.put("DNK", "208"); // Danimarca
            CountryCodeMap.put("DMA", "212"); // Dominica
            CountryCodeMap.put("ECU", "218"); // Ecuador
            CountryCodeMap.put("EGY", "818"); // Egitto
            CountryCodeMap.put("IRL", "372"); // Eire
            CountryCodeMap.put("SLV", "222"); // El Salvador
            CountryCodeMap.put("ARE", "784"); // Emirati Arabi Uniti
            CountryCodeMap.put("ERI", "232"); // Eritrea
            CountryCodeMap.put("EST", "233"); // Estonia
            CountryCodeMap.put("ETH", "231"); // Etiopia

            // F-G
            CountryCodeMap.put("RUS", "643"); // Federazione Russa
            CountryCodeMap.put("FJI", "242"); // Fiji
            CountryCodeMap.put("PHL", "608"); // Filippine
            CountryCodeMap.put("FIN", "246"); // Finlandia
            CountryCodeMap.put("FRA", "250"); // Francia
            CountryCodeMap.put("GAB", "266"); // Gabon
            CountryCodeMap.put("GMB", "270"); // Gambia
            CountryCodeMap.put("GEO", "268"); // Georgia
            CountryCodeMap.put("DEU", "276"); // Germania
            CountryCodeMap.put("GHA", "288"); // Ghana
            CountryCodeMap.put("JAM", "388"); // Giamaica
            CountryCodeMap.put("JPN", "392"); // Giappone
            CountryCodeMap.put("GIB", "292"); // Gibilterra
            CountryCodeMap.put("DJI", "262"); // Gibuti
            CountryCodeMap.put("JOR", "400"); // Giordania
            CountryCodeMap.put("GRC", "300"); // Grecia
            CountryCodeMap.put("GRD", "308"); // Grenada
            CountryCodeMap.put("GRL", "304"); // Groenlandia
            CountryCodeMap.put("GLP", "312"); // Guadalupa
            CountryCodeMap.put("GUM", "316"); // Guam
            CountryCodeMap.put("GTM", "320"); // Guatemala
            CountryCodeMap.put("GIN", "324"); // Guinea
            CountryCodeMap.put("GNB", "624"); // Guinea-Bissau
            CountryCodeMap.put("GNQ", "226"); // Guinea Equatoriale
            CountryCodeMap.put("GUY", "328"); // Guyana
            CountryCodeMap.put("GUF", "254"); // Guyana Francese

            // H-I
            CountryCodeMap.put("HTI", "332"); // Haiti
            CountryCodeMap.put("HND", "340"); // Honduras
            CountryCodeMap.put("HKG", "344"); // Hong Kong
            CountryCodeMap.put("IND", "356"); // India
            CountryCodeMap.put("IDN", "360"); // Indonesia
            CountryCodeMap.put("IRN", "364"); // Iran
            CountryCodeMap.put("IRQ", "368"); // Iraq
            CountryCodeMap.put("BVT", "074"); // Isola Bouvet
            CountryCodeMap.put("CXR", "162"); // Isola di Natale
            CountryCodeMap.put("HMD", "334"); // Isola Heard e Isole McDonald
            CountryCodeMap.put("CYM", "136"); // Isole Cayman
            CountryCodeMap.put("CCK", "166"); // Isole Cocos
            CountryCodeMap.put("COK", "184"); // Isole Cook
            CountryCodeMap.put("FLK", "238"); // Isole Falkland
            CountryCodeMap.put("FRO", "234"); // Isole Faroe
            CountryCodeMap.put("MHL", "584"); // Isole Marshall
            CountryCodeMap.put("MNP", "580"); // Isole Marianne Settentrionali
            CountryCodeMap.put("UMI", "581"); // Isole Minori degli Stati Uniti d'America
            CountryCodeMap.put("NFK", "574"); // Isola Norfolk
            CountryCodeMap.put("SLB", "090"); // Isole Solomon
            CountryCodeMap.put("TCA", "796"); // Isole Turks e Caicos
            CountryCodeMap.put("VIR", "850"); // Isole Vergini Americane
            CountryCodeMap.put("VGB", "092"); // Isole Vergini Britanniche
            CountryCodeMap.put("ISR", "376"); // Israele
            CountryCodeMap.put("ISL", "352"); // Islanda
            CountryCodeMap.put("ITA", "380"); // Italia

            // K-L
            CountryCodeMap.put("KAZ", "398"); // Kazakhistan
            CountryCodeMap.put("KEN", "404"); // Kenya
            CountryCodeMap.put("KGZ", "417"); // Kirghizistan
            CountryCodeMap.put("KIR", "296"); // Kiribati
            CountryCodeMap.put("KWT", "414"); // Kuwait
            CountryCodeMap.put("LAO", "418"); // Laos
            CountryCodeMap.put("LVA", "428"); // Lettonia
            CountryCodeMap.put("LSO", "426"); // Lesotho
            CountryCodeMap.put("LBN", "422"); // Libano
            CountryCodeMap.put("LBR", "430"); // Liberia
            CountryCodeMap.put("LBY", "434"); // Libia
            CountryCodeMap.put("LIE", "438"); // Liechtenstein
            CountryCodeMap.put("LTU", "440"); // Lituania
            CountryCodeMap.put("LUX", "442"); // Lussemburgo

            // M
            CountryCodeMap.put("MAC", "446"); // Macao
            CountryCodeMap.put("MKD", "807"); // Macedonia
            CountryCodeMap.put("MDG", "450"); // Madagascar
            CountryCodeMap.put("MWI", "454"); // Malawi
            CountryCodeMap.put("MDV", "462"); // Maldive
            CountryCodeMap.put("MYS", "458"); // Malesia
            CountryCodeMap.put("MLI", "466"); // Mali
            CountryCodeMap.put("MLT", "470"); // Malta
            CountryCodeMap.put("MAR", "504"); // Marocco
            CountryCodeMap.put("MTQ", "474"); // Martinica
            CountryCodeMap.put("MRT", "478"); // Mauritania
            CountryCodeMap.put("MUS", "480"); // Maurizius
            CountryCodeMap.put("MYT", "175"); // Mayotte
            CountryCodeMap.put("MEX", "484"); // Messico
            CountryCodeMap.put("MDA", "498"); // Moldavia
            CountryCodeMap.put("MCO", "492"); // Monaco
            CountryCodeMap.put("MNG", "496"); // Mongolia
            CountryCodeMap.put("MSR", "500"); // Montserrat
            CountryCodeMap.put("MOZ", "508"); // Mozambico
            CountryCodeMap.put("MMR", "104"); // Myanmar

            // N-O
            CountryCodeMap.put("NAM", "516"); // Namibia
            CountryCodeMap.put("NRU", "520"); // Nauru
            CountryCodeMap.put("NPL", "524"); // Nepal
            CountryCodeMap.put("NIC", "558"); // Nicaragua
            CountryCodeMap.put("NER", "562"); // Niger
            CountryCodeMap.put("NGA", "566"); // Nigeria
            CountryCodeMap.put("NIU", "570"); // Niue
            CountryCodeMap.put("NOR", "578"); // Norvegia
            CountryCodeMap.put("NCL", "540"); // Nuova Caledonia
            CountryCodeMap.put("NZL", "554"); // Nuova Zelanda
            CountryCodeMap.put("OMN", "512"); // Oman

            // P-Q
            CountryCodeMap.put("NLD", "528"); // Paesi Bassi
            CountryCodeMap.put("PAK", "586"); // Pakistan
            CountryCodeMap.put("PLW", "585"); // Palau
            CountryCodeMap.put("PAN", "591"); // Panamá
            CountryCodeMap.put("PNG", "598"); // Papua Nuova Guinea
            CountryCodeMap.put("PRY", "600"); // Paraguay
            CountryCodeMap.put("PER", "604"); // Perù
            CountryCodeMap.put("PCN", "612"); // Pitcairn
            CountryCodeMap.put("PYF", "258"); // Polinesia Francese
            CountryCodeMap.put("POL", "616"); // Polonia
            CountryCodeMap.put("PRT", "620"); // Portogallo
            CountryCodeMap.put("PRI", "630"); // Porto Rico
            CountryCodeMap.put("QAT", "634"); // Qatar

            // R
            CountryCodeMap.put("GBR", "826"); // Regno Unito
            CountryCodeMap.put("CZE", "203"); // Repubblica Ceca
            CountryCodeMap.put("CAF", "140"); // Repubblica Centroafricana
            CountryCodeMap.put("COG", "178"); // Repubblica del Congo
            CountryCodeMap.put("COD", "180"); // Repubblica Democratica del Congo
            CountryCodeMap.put("DOM", "214"); // Repubblica Dominicana
            CountryCodeMap.put("REU", "638"); // Reunion
            CountryCodeMap.put("ROU", "642"); // Romania
            CountryCodeMap.put("RWA", "646"); // Ruanda

            // S
            CountryCodeMap.put("ESH", "732"); // Sahara Occidentale
            CountryCodeMap.put("KNA", "659"); // Saint Kitts e Nevis
            CountryCodeMap.put("SPM", "666"); // Saint Pierre e Miquelon
            CountryCodeMap.put("VCT", "670"); // Saint Vincent e Grenadine
            CountryCodeMap.put("WSM", "882"); // Samoa
            CountryCodeMap.put("ASM", "016"); // Samoa Americane
            CountryCodeMap.put("SMR", "674"); // San Marino
            CountryCodeMap.put("SHN", "654"); // Sant'Elena
            CountryCodeMap.put("LCA", "662"); // Santa Lucia
            CountryCodeMap.put("STP", "678"); // Sao Tome e Principe
            CountryCodeMap.put("SEN", "686"); // Senegal
            CountryCodeMap.put("SCG", "891"); // Serbia e Montenegro
            CountryCodeMap.put("SYC", "690"); // Seychelles
            CountryCodeMap.put("SLE", "694"); // Sierra Leone
            CountryCodeMap.put("SGP", "702"); // Singapore
            CountryCodeMap.put("SYR", "760"); // Siria
            CountryCodeMap.put("SVK", "703"); // Slovacchia
            CountryCodeMap.put("SVN", "705"); // Slovenia
            CountryCodeMap.put("SOM", "706"); // Somalia
            CountryCodeMap.put("ESP", "724"); // Spagna
            CountryCodeMap.put("LKA", "144"); // Sri Lanka
            CountryCodeMap.put("FSM", "583"); // Stati Federati della Micronesia
            CountryCodeMap.put("USA", "840"); // Stati Uniti d'America
            CountryCodeMap.put("ZAF", "710"); // Sud Africa
            CountryCodeMap.put("SGS", "239"); // Sud Georgia e Isole Sandwich
            CountryCodeMap.put("SDN", "736"); // Sudan
            CountryCodeMap.put("SUR", "740"); // Suriname
            CountryCodeMap.put("SJM", "744"); // Svalbard e Jan Mayen
            CountryCodeMap.put("SWE", "752"); // Svezia
            CountryCodeMap.put("CHE", "756"); // Svizzera
            CountryCodeMap.put("SWZ", "748"); // Swaziland

            // T-Z
            CountryCodeMap.put("TIK", "762"); // Tagikistan
            CountryCodeMap.put("THA", "764"); // Tailandia
            CountryCodeMap.put("TWN", "158"); // Taiwan
            CountryCodeMap.put("TZA", "834"); // Tanzania
            CountryCodeMap.put("IOT", "092"); // Territori Britannici dell'Oceano Indiano
            CountryCodeMap.put("ATF", "260"); // Territori Francesi del Sud
            CountryCodeMap.put("PSE", "275"); // Territori Palestinesi Occupati
            CountryCodeMap.put("TLS", "626"); // Timor Est
            CountryCodeMap.put("TGO", "768"); // Togo
            CountryCodeMap.put("TKL", "772"); // Tokelau
            CountryCodeMap.put("ΤΟΝ", "776"); // Tonga
            CountryCodeMap.put("TTO", "780"); // Trinidad e Tobago
            CountryCodeMap.put("TUN", "788"); // Tunisia
            CountryCodeMap.put("TUR", "792"); // Turchia
            CountryCodeMap.put("TKM", "795"); // Turkmenistan
            CountryCodeMap.put("TUV", "798"); // Tuvalu
            CountryCodeMap.put("UKR", "804"); // Ucraina
            CountryCodeMap.put("UGA", "800"); // Uganda
            CountryCodeMap.put("HUN", "348"); // Ungheria
            CountryCodeMap.put("URY", "858"); // Uruguay
            CountryCodeMap.put("UZB", "860"); // Uzbekistan
            CountryCodeMap.put("VUT", "548"); // Vanuatu
            CountryCodeMap.put("VEN", "862"); // Venezuela
            CountryCodeMap.put("VNM", "704"); // Vietnam
            CountryCodeMap.put("WLF", "876"); // Wallis e Futuna
            CountryCodeMap.put("YEM", "887"); // Yemen
            CountryCodeMap.put("ZMB", "894"); // Zambia
            CountryCodeMap.put("ZWE", "716"); // Zimbabwe
        }

        /**
         * Restituisce il codice numerico ISO 3166-1 corrispondente al codice alpha-3 fornito.
         * @param alpha3 Il codice a tre lettere (es. "ITA").
         * @return Il codice numerico a tre cifre o "Unknown" se non trovato.
         */
        public static String getNumericCountryCode(String alpha3) {
            if (alpha3 == null) {
                return "Unknown";
            }

            // Cerca nella mappa (gestisce l'input in modo case-insensitive)
            String result = CountryCodeMap.get(alpha3.toUpperCase());

            return (result != null) ? result : "Unknown";
        }


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
        String tipoComunicazione = "1";
        sb.append(tipoComunicazione);

        if (tipoComunicazione.equals("1")) {
            LocalDate now = LocalDate.now();
            String year = now.format(DateTimeFormatter.ofPattern("yyyy"));
            String month = now.format(DateTimeFormatter.ofPattern("MM"));
            sb.append(year).append(month);
        } else {
            sb.append("0000");
            sb.append("00");
        }


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
        sb.append(rightPad("NEXI PAYMENTS SPA", 70, ' '));

        // 16. Comune sede legale (positions 227-266, length 40)
        sb.append(rightPad("MILANO", 40, ' '));

        // 17. Provincia sede legale (positions 267-268, length 2)
        sb.append("MI");

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

        // 3. Tipo rapporto (positions 52-53, length 2)
        sb.append(rapporti.getTipoRapportoInterno().substring(1));

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
            sb.append(dataInizio);
        }

        // 6. Data fine rapporto (positions 86-93, length 8, format DDMMYYYY)
//            String dataFine = nullSafe(rapporti.getDataFineRapporto()).trim();
//            if (dataFine.isEmpty()) {
        sb.append("01010001");
//            } else {
        // Convert from YYYYMMDD to DDMMYYYY
//                sb.append(convertDateFormat(dataFine));
//            }

        // 7. CAB (positions 94-98, length 5)
        // Mandatory for tipo rapporto 1, 2, 3, 12, 13, 96, 97
        // If stored as letters, convert to numeric code; if stored as numbers, leave as-is
        String cab = nullSafe(rapporti.getCab()).trim();
        if (cab.isEmpty()) {
            sb.append(" ".repeat(5));
        } else if (cab.matches("^[0-9]+$")) {
            // Already numeric, use as-is (padded to 5 characters)
            sb.append(rightPad(cab, 5, ' '));
        } else {
            // Letters, convert to numeric code
            sb.append(rightPad(getNumericCountryCode(cab), 5, ' '));
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
     * Section 2 (Type 2): Collegamenti + Rapporti + Soggetti
     * Format: 398 characters + end-of-line
     */
    public static String toAnagraficaOutputString(Collegamenti collegamenti, int count) {
        StringBuilder sb = new StringBuilder(398);

        Rapporti rapporto = collegamenti.getRapporto();
        Soggetti soggetto = collegamenti.getSoggetto();

        // 1. Tipo record (position 1, length 1): always "2"
        sb.append("2");

        // 2. Codice univoco rapporto (positions 2-51, length 50)
        sb.append(rightPad(nullSafe(collegamenti.getChiaveRapporto()), 50, ' '));

        // 3. Progressivo anagrafica (positions 52-60, length 9)
        // Progressive counter starting from "000000001"
        int progressiveCounter = count + 1;
        sb.append(leftPad(Integer.toString(progressiveCounter), 9, '0'));

        if (rapporto != null) {
            // 4. Data inizio partecipazione (positions 61-68, length 8, format DDMMYYYY)
            String dataInizio = nullSafe(rapporto.getDataInizioRapporto()).trim();
            if (dataInizio.isEmpty()) {
                sb.append("01010001");
            } else {
                sb.append(dataInizio);
            }

            // 5. Data fine partecipazione (positions 69-76, length 8, format DDMMYYYY)
//            String dataFine = nullSafe(rapporto.getDataFineRapporto()).trim();
//            if (dataFine.isEmpty()) {
            sb.append("01010001");
//            } else {
//                sb.append(convertDateFormat(dataFine));
//            }
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
                    sb.append(dataNascita);
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
    public static String toSaldiEMovementiOutputString(Collegamenti collegamenti) {
        StringBuilder sb = new StringBuilder(398);

        Rapporti rapporto = collegamenti.getRapporto();
        DatiContabili datiContabili = collegamenti.getDatiContabili();

        // 1. Tipo record (position 1, length 1): always "3"
        sb.append("3");

        // 2. Codice univoco rapporto (positions 2-51, length 50)
        sb.append(rightPad(nullSafe(collegamenti.getChiaveRapporto()), 50, ' '));

        if (rapporto != null) {
            // 3. Tipo rapporto (positions 52-53, length 2)
            sb.append(rapporto.getTipoRapportoInterno().substring(1));

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
        sb.append(" ".repeat(2));

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


