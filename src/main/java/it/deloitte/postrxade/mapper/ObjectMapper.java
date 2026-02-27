package it.deloitte.postrxade.mapper;

import it.deloitte.postrxade.entity.SubmissionStatus;
import it.deloitte.postrxade.parser.transaction.MerchantRecord;
import it.deloitte.postrxade.parser.transaction.TransactionRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import ma.glasnost.orika.MapperFacade;
import ma.glasnost.orika.MapperFactory;
import ma.glasnost.orika.impl.DefaultMapperFactory;

import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.dto.*;

/**
 * Configuration component for Orika Bean Mapping.
 * <p>
 * This class defines how Entities map to DTOs. It provides multiple
 * {@link MapperFactory} configurations
 * to handle different serialization needs (e.g., full object graphs vs.
 * simplified views to avoid circular dependencies).
 */
@Component
public class ObjectMapper {

    /**
     * The primary MapperFactory used for standard, full-depth mappings.
     * <p>
     * This factory includes all fields by default, suitable for detailed views
     * where
     * the complete object graph is required.
     *
     * @return A configured {@link MapperFactory}.
     */
    @Bean
    public MapperFactory mapperFactory() {
        MapperFactory mapperFactory = new DefaultMapperFactory.Builder()
                // Java 21: avoids Orika CloneableConverter reflective access to Object.clone()
                .useBuiltinConverters(false)
                .build();

        // User ↔ UserDTO mapping
        mapperFactory.classMap(User.class, UserDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("firstName", "firstName")
                .field("lastName", "lastName")
                .field("email", "email")
                .field("company", "company")
                .field("office", "office")
                .field("lastLoggedIn", "lastLoggedIn")
                .field("authorities", "authorities")
                .byDefault()
                .register();

        // Authority ↔ AuthorityDTO mapping
        mapperFactory.classMap(Authority.class, AuthorityDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("description", "description")
                // .field("users", "users")
                .byDefault()
                .register();

        // Period ↔ PeriodDTO mapping
        mapperFactory.classMap(Period.class, PeriodDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("name", "name")
                .field("description", "description")
                .field("order", "order")
                .field("isActive", "isActive")
                .field("obligations", "obligations")
                .byDefault()
                .register();

        // Obligation ↔ ObligationDTO mapping
        mapperFactory.classMap(Obligation.class, ObligationDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("period", "period")
                .field("fiscalYear", "fiscalYear")
                .field("submissions", "submissions")
                .byDefault()
                .register();

        // SubmissionStatus ↔ SubmissionStatusDTO mapping
        mapperFactory.classMap(SubmissionStatus.class, SubmissionStatusDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("name", "name")
                .field("description", "description")
                .field("order", "order")
                // .field("submissions", "submissions")
                // .field("beforeLogs", "beforeLogs")
                // .field("afterLogs", "afterLogs")
                .byDefault()
                .register();

        // Submission ↔ SubmissionDTO mapping
        mapperFactory.classMap(Submission.class, SubmissionDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("currentSubmissionStatus", "currentSubmissionStatus")
                .field("lastUpdateBy", "lastUpdateBy")
                .field("obligation", "obligation")
                .field("lastUpdatedAt", "lastUpdatedAt")
                .field("approvedAt", "approvedAt")
                .field("deadlineDate", "deadlineDate")
                .field("cancelledAt", "cancelledAt")
                .field("lastSubmissionStatus", "lastSubmissionStatus")
                .field("isManual", "isManual")
                .field("ingestions", "ingestions")
                .field("outputs", "outputs")
                .field("logs", "logs")
                .byDefault()
                .register();

        // IngestionType ↔ IngestionTypeDTO mapping
        mapperFactory.classMap(IngestionType.class, IngestionTypeDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("name", "name")
                .field("description", "description")
                .field("order", "order")
                .field("isActive", "isActive")
                .field("ingestions", "ingestions")
                .byDefault()
                .register();

        // IngestionStatus ↔ IngestionStatusDTO mapping
        mapperFactory.classMap(IngestionStatus.class, IngestionStatusDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("name", "name")
                .field("description", "description")
                .field("order", "order")
                .byDefault()
                .register();

        // Ingestion ↔ IngestionDTO mapping
        mapperFactory.classMap(Ingestion.class, IngestionDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("ingestionType", "ingestionType")
                .field("ingestionStatus", "ingestionStatus")
                .field("submission", "submission")
                .field("ingestionError", "ingestionError")
                .field("ingestedAt", "ingestedAt")
                .field("fullPath", "fullPath")
                .field("transactions", "transactions")
                .byDefault()
                .register();

        // Output ↔ OutputDTO mapping
        mapperFactory.classMap(Output.class, OutputDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("submission", "submission")
                .field("fullPath", "fullPath")
                .field("extensionType", "extensionType")
                .field("generatedAt", "generatedAt")
                .byDefault()
                .register();

        // TransactionError and Currency mappings removed as entities were deleted

        // Log ↔ LogDTO mapping
        mapperFactory.classMap(Log.class, LogDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                // .field("timestamp", "timestamp")
                .field("submission", "submission")
                .field("updater", "updater")
                .field("beforeSubmissionStatus", "beforeSubmissionStatus")
                .field("afterSubmissionStatus", "afterSubmissionStatus")
                .field("message", "message")
                .byDefault()
                .register();

        mapperFactory.classMap(TransactionRecord.class, Transaction.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("tipoRecord", "tpRec")
                .field("intermediario", "idIntermediario")
                .field("idEsercente", "idEsercente")
                .field("chiaveRapportoBanca", "chiaveBanca")
                .field("terminalId", "idPos")
                .field("tipoOperazione", "tipoOpe")
                .field("dataOperazione", "dtOpe")
                .field("divisaOperazioni", "divisaOpe")
                .field("tipoPagamento", "tipoPag")
                .field("importoTotaleOperazioni", "impOpe")
                .field("numeroOperazioniGiorno", "totOpe")
                .byDefault()
                .register();

        mapperFactory.classMap(TransactionRecord.class, ResolvedTransaction.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("tipoRecord", "tpRec")
                .field("intermediario", "idIntermediario")
                .field("idEsercente", "idEsercente")
                .field("chiaveRapportoBanca", "chiaveBanca")
                .field("terminalId", "idPos")
                .field("tipoOperazione", "tipoOpe")
                .field("dataOperazione", "dtOpe")
                .field("divisaOperazioni", "divisaOpe")
                .field("tipoPagamento", "tipoPag")
                .field("importoTotaleOperazioni", "impOpe")
                .field("numeroOperazioniGiorno", "totOpe")
                .byDefault()
                .register();

        mapperFactory.classMap(MerchantRecord.class, Merchant.class)
                .mapNulls(false)
                .mapNullsInReverse(false)
                .field("tipoRecord", "tpRec")
                .field("intermediario", "idIntermediario")
                .field("idEsercente", "idEsercente")
                .field("codiceFiscaleEsercente", "codFiscale")
                .field("partitaIva", "partitaIva")
                .field("idSalmov", "idSalmov")
                .byDefault()
                .register();

        // Transaction ↔ TransactionDTO mapping
        mapperFactory.classMap(Transaction.class, TransactionDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("ingestion", "ingestion")
                .field("tpRec", "tpRec")
                .field("idIntermediario", "idIntermediario")
                .field("idEsercente", "idEsercente")
                .field("chiaveBanca", "chiaveBanca")
                .field("idPos", "idPos")
                .field("tipoOpe", "tipoOpe")
                .field("dtOpe", "dtOpe")
                .field("divisaOpe", "divisaOpe")
                .field("tipoPag", "tipoPag")
                .field("impOpe", "impOpe")
                .field("totOpe", "totOpe")
                .field("merchant", "merchant")
                .byDefault()
                .register();

        // IngestionError ↔ IngestionErrorDTO mapping
        mapperFactory.classMap(IngestionError.class, IngestionErrorDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("name", "name")
                .field("description", "description")
                .field("order", "order")
                .field("isActive", "isActive")
                .field("ingestions", "ingestions")
                .byDefault()
                .register();

        // ErrorType ↔ ErrorTypeDTO mapping
        mapperFactory.classMap(ErrorType.class, ErrorTypeDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("name", "name")
                .field("description", "description")
                .field("severityLevel", "severityLevel")
                .field("isActive", "isActive")
                // .field("errorRecords", "errorRecords")
                .byDefault()
                .register();

        // Merchant ↔ MerchantDTO mapping
        mapperFactory.classMap(Merchant.class, MerchantDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("ingestion", "ingestion")
                .field("tpRec", "tpRec")
                .field("idIntermediario", "idIntermediario")
                .field("idEsercente", "idEsercente")
                .field("codFiscale", "codFiscale")
                .field("partitaIva", "partitaIva")
                .field("transactions", "transactions")
                .byDefault()
                .register();

        // ErrorRecord ↔ ErrorRecordDTO mapping
        mapperFactory.classMap(ErrorRecord.class, ErrorRecordDTO.class)
                .mapNulls(false).mapNullsInReverse(false)
                .field("id", "id")
                .field("ingestion", "ingestion")
                // .field("errorType", "errorType")
                // .field("errorMessage", "errorMessage")
                .field("rawRow", "rawRow")
                .byDefault()
                .register();

        return mapperFactory;
    }

    /**
     * Creates the primary {@link MapperFacade} bean.
     * <p>
     * This is the bean commonly injected into services using
     * {@code @Autowired MapperFacade}.
     *
     * @param mapperFactory The default factory configured above.
     * @return The facade for performing mappings.
     */
    @Bean
    @Qualifier("mapperFacade")
    public MapperFacade mapperFacade(MapperFactory mapperFactory) {
        return mapperFactory.getMapperFacade();
    }

    /**
     * Creates an "Alternative" MapperFactory.
     * <p>
     * This factory is configured with explicit <strong>exclusions</strong>.
     * It is used when generating "Custom" DTOs or lists where infinite recursion
     * (circular dependencies)
     * must be avoided (e.g., User -> Authority -> User -> Authority).
     *
     * @return A configured {@link MapperFactory} for simplified views.
     */
    @Bean
    @Qualifier("alternativeMapperFactory")
    public MapperFactory alternativeMapperFactory() {
        MapperFactory alternativeMapperFactory = new DefaultMapperFactory.Builder()
                // Java 21: avoids Orika CloneableConverter reflective access to Object.clone()
                .useBuiltinConverters(false)
                .build();

        // Alternative mappings with exclusions for circular references
        alternativeMapperFactory.classMap(User.class, UserDTO.class)
                .exclude("authorities")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(Submission.class, SubmissionCustomDTO.class)
                .exclude("ingestions")
                .exclude("lastUpdateBy")
                .exclude("obligation")
                .exclude("lastUpdatedAt")
                .exclude("batchId")
                .exclude("submissionType")
                .exclude("outputs")
                .exclude("logs")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(Authority.class, AuthorityDTO.class)
                .exclude("users")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(Period.class, PeriodDTO.class)
                .exclude("obligations")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(Obligation.class, ObligationDTO.class)
                .exclude("submissions")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(SubmissionStatus.class, SubmissionStatusDTO.class)
                .field("id", "id")
                .field("name", "name")
                .field("description", "description")
                .field("order", "order")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .register();

        alternativeMapperFactory.classMap(SubmissionStatusGroup.class, SubmissionStatusGroupDTO.class)
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(Submission.class, SubmissionDTO.class)
                .exclude("ingestions")
                .exclude("outputs")
                .exclude("logs")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(IngestionType.class, IngestionTypeDTO.class)
                .exclude("ingestions")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(IngestionStatus.class, IngestionStatusDTO.class)
                .exclude("ingestions")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(Ingestion.class, IngestionDTO.class)
                .exclude("transactions")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        // TransactionError mapping removed as entity was deleted

        alternativeMapperFactory.classMap(Log.class, LogDTO.class)
                .exclude("submission")
                .exclude("updater")
                .exclude("beforeSubmissionStatus")
                .exclude("afterSubmissionStatus")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .register();

        alternativeMapperFactory.classMap(Transaction.class, TransactionDTO.class)
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        alternativeMapperFactory.classMap(Output.class, OutputDTO.class)
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        // Currency mapping removed as entity was deleted

        return alternativeMapperFactory;
    }

    /**
     * Creates a specialized mapper for User Services.
     * <p>
     * Excludes 'checklists' from UserDTO to provide a user view focused on identity
     * permissions.
     *
     * @return A configured {@link MapperFactory}.
     */
    @Bean
    @Qualifier("userServiceMapperFactory")
    public MapperFactory userServiceMapperFactory() {
        MapperFactory mapperFactory = new DefaultMapperFactory.Builder()
                // Java 21: avoids Orika CloneableConverter reflective access to Object.clone()
                .useBuiltinConverters(false)
                .build();

        mapperFactory.classMap(User.class, UserDTO.class)
                .exclude("checklists")
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        mapperFactory.classMap(Authority.class, AuthorityDTO.class)
                .mapNulls(false)
                .mapNullsInReverse(false)
                .byDefault()
                .register();

        return mapperFactory;
    }

    /**
     * Creates the Facade for the Alternative Mapper.
     * <p>
     * Inject this using
     * {@code @Autowired @Qualifier("alternativeMapperFacade") MapperFacade}.
     *
     * @param alternativeMapperFactory The factory created above.
     * @return The facade for performing simplified mappings.
     */
    @Bean
    @Qualifier("alternativeMapperFacade")
    public MapperFacade alternativeMapperFacade(MapperFactory alternativeMapperFactory) {
        return alternativeMapperFactory.getMapperFacade();
    }
}
