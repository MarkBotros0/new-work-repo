package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.entity.Authority;
import it.deloitte.postrxade.entity.User;
import it.deloitte.postrxade.enums.AuthIdProfilo;
import it.deloitte.postrxade.exception.ActionNotPermittedException;
import it.deloitte.postrxade.exception.AuthorityCodeNotValidException;
import it.deloitte.postrxade.exception.UserNotValidException;
import it.deloitte.postrxade.repository.AuthorityRepository;
import it.deloitte.postrxade.repository.UserRepository;
import it.deloitte.postrxade.service.AuthorityService;

import ma.glasnost.orika.MapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

//import javax.annotation.PostConstruct;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Implementation of {@link AuthorityService} for managing {@link Authority}.
 */
@Service
public class AuthorityServiceImpl implements AuthorityService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorityServiceImpl.class);
    private static final String LOGGER_MSG_BEGIN = "{}Inizio [hashcode={}]";
    private static final String LOGGER_MSG_END = "{}Fine";

    @Value("${application.authority.info-env}")
    private String cfgAuthorityInfoEnv; //	ASK questo ti serve per anche le proprieta del server SMTP

    private static final String AUTH_TOKEN_SEPARATOR = "_";
    private static final String AUTH_APP_PREFIX = "ICS";

    private static final String AUTH_ID_PROFILO_AUDITOR = AuthIdProfilo.AUDITOR.getAuthCode();
    private static final String AUTH_ID_PROFILO_MANAGER = AuthIdProfilo.MANAGER.getAuthCode();
    private static final String AUTH_ID_PROFILO_REVIEWER = AuthIdProfilo.REVIEWER.getAuthCode();
    private static final String AUTH_ID_PROFILO_APPROVER = AuthIdProfilo.APPROVER.getAuthCode();
    private static final String AUTH_ID_PROFILO_SUPER = AuthIdProfilo.SURER.getAuthCode();

    private static final String AUTH_CODE_REGEX = "[A-Z0-9-]{1,9}";
    @Autowired
    private UserRepository userRepository;
    @Autowired
    private AuthorityRepository authorityRepository;
    @Autowired
    MapperFactory mapperFactory;

    //@PostConstruct
    public void init() {
        String funcIdentifier = "[AUTHSIinit] ";
        LOGGER.debug(LOGGER_MSG_BEGIN, funcIdentifier, this.hashCode());
        LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
    }

    public Authority getAuthorityFromCode(String code) throws AuthorityCodeNotValidException {
        String funcIdentifier = "[GAUTH-" + code.replace("", "_").replace("/ ", "_") + "] ";
        LOGGER.debug(LOGGER_MSG_BEGIN, funcIdentifier, this.hashCode());
        Authority result = null;

        LOGGER.debug("{}cfgAuthorityInfoEnv={}", funcIdentifier, cfgAuthorityInfoEnv);

        if (code == null) {
            String errorPayload = "{}Authority code e' NULL";
            LOGGER.warn(errorPayload, funcIdentifier);
            errorPayload = "Authority code e' NULL";
            throw new AuthorityCodeNotValidException(errorPayload);
        }

//		List<String> elements = getTokensWithCollection(code);
//		LOGGER.debug("{}elements={}", funcIdentifier, elements);


        LOGGER.debug("{}profiloCode={}", funcIdentifier, code);

        if (!code.equals(AUTH_ID_PROFILO_SUPER)
                && !code.equals(AUTH_ID_PROFILO_APPROVER)
                && !code.equals(AUTH_ID_PROFILO_MANAGER)
                && !code.equals(AUTH_ID_PROFILO_REVIEWER)
                && !code.equals(AUTH_ID_PROFILO_AUDITOR)
        ) {
            String errorPayload = "{}Authority code non valido: codice profilo non riconosciuto [code='" + code + "']";
            LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
            errorPayload = "Authority code non valido: codice profilo non riconosciuto [code='" + code + "']";
            throw new AuthorityCodeNotValidException(errorPayload);
        }

//		LegalEntityParentCompany legalEntityParentCompany = null;
//		Long legaleEntityParentCompanyId = null;
//
//		LegalEntitySubsidiary legalEntitySubsidiary = null;
//		Long legalEntitySubsidiaryId = null;
//
//		LegalEntityCompany legalEntityCompany = null;
//		Long legaleEntityCompanyId = null;
//
//		ControlFunction controlFunction = null;
//		Long controlFunctionId = null;
//
//		//TODO modifica codice seguente quando i tag saranno i nuovi
//		/* Il presupposto qui è che possono anche funzionare tag con un numero inferiore di 7 per i ST cosa che in realtà non sarà possibile
//		* */
//
//		if (elements.size() >= 4 && !elements.get(3).equals("ALL")) {
//			//se invece e' ALL mi aspetto che dopo  ci
//			// siano altri all. per i casi ALL in app l'auth viene gestita lasciata a NULL la property relativa alla
//			// LE selezionata
//			String legaleEntityParentCompanyCode = elements.get(3);
//			LOGGER.debug("{}legalEntityParentCompanyCode={}", funcIdentifier, legaleEntityParentCompanyCode);
//
//			if (!Pattern.matches(AUTH_CODE_REGEX, legaleEntityParentCompanyCode)) {
//				String errorPayload = "{}Authority code non valido: codice legal entity parent company non riconosciuto [code='" + code + "']";
//				LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
//				errorPayload = "Authority code non valido: codice legal entity parent company non riconosciuto [code='" + code + "']";
//				throw new AuthorityCodeNotValidException(errorPayload);
//			}
//
//			List<LegalEntityParentCompany> legalEntityParentCompanys = legalEntityParentCompanyRepository.findByAuthorityCodeAndActive(legaleEntityParentCompanyCode, true);
//			if (legalEntityParentCompanys != null && !legalEntityParentCompanys.isEmpty()) {
//				legalEntityParentCompany = legalEntityParentCompanys.get(0); //ci aspetiamo di trovarne sempre 1
//				legaleEntityParentCompanyId = legalEntityParentCompany.getId();
//				LOGGER.debug("{}legalEntityParentCompanyId={}", funcIdentifier, legaleEntityParentCompanyId);
//			}
//			else {
//				String errorPayload = "{}Authority code non valido: codice legal entity parent company non trovato o non attivo [code='" + code + "']";
//				LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
//				errorPayload = "Authority code non valido: codice legal entity parent company non trovato o non attivo [code='" + code + "']";
//				throw new AuthorityCodeNotValidException(errorPayload);
//			}
//		}
//
//		if (elements.size() >= 5 && !elements.get(4).equals("ALL")) {
//			String legaleEntitySubsidiaryCode = elements.get(4);
//			LOGGER.debug("{}legalEntitySubsidiaryCode={}", funcIdentifier, legaleEntitySubsidiaryCode);
//
//			if (!Pattern.matches(AUTH_CODE_REGEX, legaleEntitySubsidiaryCode)) {
//				String errorPayload = "{}Authority code non valido: codice legal entity subsidiary non riconosciuto o non attivo [code='" + code + "']";
//				LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
//				errorPayload = "Authority code non valido: codice legal entity subsidiary non riconosciut o non attivoo [code='" + code + "']";
//				throw new AuthorityCodeNotValidException(errorPayload);
//			}
//
//			List<LegalEntitySubsidiary> legalEntitySubsidiaries = legalEntitySubsidiaryRepository.findByAuthorityCodeAndActive(legaleEntitySubsidiaryCode, true);
//			if (legalEntitySubsidiaries != null && !legalEntitySubsidiaries.isEmpty()) {
//				legalEntitySubsidiary = legalEntitySubsidiaries.get(0);
//				legalEntitySubsidiaryId = legalEntitySubsidiary.getId();
//				LOGGER.debug("{}legalEntitySubsidiaryId={}", funcIdentifier, legalEntitySubsidiaryId);
//			}
//			else {
//				String errorPayload = "{}Authority code non valido: codice legal entity subsidiary non trovato o non attivo [code='" + code + "']";
//				LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
//				errorPayload = "Authority code non valido: codice legal entity subsidiary non trovato o non attivo [code='" + code + "']";
//				throw new AuthorityCodeNotValidException(errorPayload);
//			}
//		}
//
//		if (elements.size() >= 6 && !elements.get(5).equals("ALL")) {
//			String legaleEntityCompanyCode = elements.get(5);
//			LOGGER.debug("{}legalEntityCompanyCode={}", funcIdentifier, legaleEntityCompanyCode);
//
//			if (!Pattern.matches(AUTH_CODE_REGEX, legaleEntityCompanyCode)) {
//				String errorPayload = "{}Authority code non valido: codice legal entity company non riconosciuto o non attivo [code='" + code + "']";
//				LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
//				errorPayload = "Authority code non valido: codice legal entity company non riconosciuto o non attivo [code='" + code + "']";
//				throw new AuthorityCodeNotValidException(errorPayload);
//			}
//
//			List<LegalEntityCompany> legalEntityCompanies = legalEntityCompanyRepository.findByAuthorityCodeAndActive(legaleEntityCompanyCode, true);
//			if (legalEntityCompanies != null && !legalEntityCompanies.isEmpty()) {
//				legalEntityCompany = legalEntityCompanies.get(0);
//				legaleEntityCompanyId = legalEntityCompany.getId();
//				LOGGER.debug("{}legalEntityCompanyId={}", funcIdentifier, legaleEntityCompanyId);
//			}
//			else {
//				String errorPayload = "{}Authority code non valido: codice legal entity company non trovato o non attivo [code='" + code + "']";
//				LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
//				errorPayload = "Authority code non valido: codice legal entity company non trovato o non attivo [code='" + code + "']";
//				throw new AuthorityCodeNotValidException(errorPayload);
//			}
//		}
//
//		if (elements.size() == 7 && !elements.get(6).equals("ALL")) {
//			String controlFuncCode = elements.get(6);
//			LOGGER.debug("{}controlfuncCode={}", funcIdentifier, controlFuncCode);
//
//			if (!Pattern.matches(AUTH_CODE_REGEX, controlFuncCode)) {
//				String errorPayload = "{}Authority code non valido: codice controlfunc non riconosciuto o non attivo [code='" + code + "']";
//				LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
//				errorPayload = "Authority code non valido: codice codice controlfunc non riconosciuto o non attivo [code='" + code + "']";
//				throw new AuthorityCodeNotValidException(errorPayload);
//			}
//
//			if (controlFuncCode.equals("NONE")){
//				List<ControlFunction> controlFunctions = controlFunctionRepository.findByAuthorityCode(controlFuncCode);
//				if (controlFunctions != null && !controlFunctions.isEmpty()){
//					controlFunction = controlFunctions.get(0);
//					controlFunctionId = controlFunction.getId();
//					LOGGER.debug("{}controlFunction={}", funcIdentifier, controlFunctionId);
//				}
//				else{
//					String errorPayload = "{}Authority code non valido: codice control func non trovato [code='" + code + "']";
//					LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
//					errorPayload = "Authority code non valido: codice control func non trovato [code='" + code + "']";
//					throw new AuthorityCodeNotValidException(errorPayload);
//				}
//
//			} else {
//				List<ControlFunction> activeControlFunctions = controlFunctionRepository.findByAuthorityCodeAndActive(controlFuncCode, true);
//				if (activeControlFunctions != null && !activeControlFunctions.isEmpty()){
//					controlFunction = activeControlFunctions.get(0);
//					controlFunctionId = controlFunction.getId();
//					LOGGER.debug("{}controlFunction={}", funcIdentifier, controlFunctionId);
//				}
//				else{
//					String errorPayload =
//							"{}Authority code non valido: codice control func non trovato o non attivo [code='" + code + "']";
//					LOGGER.warn(errorPayload.replace("", "_").replace("/ ", "_"), funcIdentifier);
//					errorPayload = "Authority code non valido: codice control func non trovato o non attivo [code='" + code + "']";
//					throw new AuthorityCodeNotValidException(errorPayload);
//				}
//
//			}
//
//
//		}
//
        String description = "";
        if (code.equals(AUTH_ID_PROFILO_AUDITOR)) {
            description = AuthIdProfilo.AUDITOR.getDescription();
        }
        if (code.equals(AUTH_ID_PROFILO_MANAGER)) {
            description = AuthIdProfilo.MANAGER.getDescription();
        }
        if (code.equals(AUTH_ID_PROFILO_REVIEWER)) {
            description = AuthIdProfilo.REVIEWER.getDescription();
        }
        if (code.equals(AUTH_ID_PROFILO_APPROVER)) {
            description = AuthIdProfilo.APPROVER.getDescription();
        }
        if (code.equals(AUTH_ID_PROFILO_SUPER)) {
            description = AuthIdProfilo.SURER.getDescription();
        }

        LOGGER.debug("{}Call to [authorityRepository.findById({})]", funcIdentifier, code);
        Optional<Authority> authority = authorityRepository.findById(code);
        if (authority.isPresent()) {
            LOGGER.debug("{}authority id:'{}' found", funcIdentifier, code);
            result = authority.get();

            result.setDescription(description);
            LOGGER.debug("{}Call to [authorityRepository.save(result)]", funcIdentifier);
            result = authorityRepository.save(result);
            LOGGER.debug("{}result={}", funcIdentifier, result);
        } else {
            LOGGER.debug("{}authority id:'{}' not found", funcIdentifier, code);
            result = new Authority();
            result.setId(code);
            result.setDescription(description);
            LOGGER.debug("{}Call to [authorityRepository.save(result)]", funcIdentifier);
            result = authorityRepository.save(result);
            LOGGER.debug("{}result={}", funcIdentifier, result);
        }

        LOGGER.debug(LOGGER_MSG_END, funcIdentifier);
        return result;
    }

    private List<String> getTokensWithCollection(String str) {
        return Collections.list(new StringTokenizer(str, AUTH_TOKEN_SEPARATOR))
                .stream()
                .map(token -> (String) token)
                .collect(Collectors.toList());
    }


    @Override
    public void checkUserAuthority(String userId, Set<String> authorizedCodes) throws UserNotValidException, ActionNotPermittedException {
        User user = userRepository.findByIdWithAuthorities(userId)
                .orElseThrow(() -> new UserNotValidException("User with user Id: " + userId + " is not found"));

        boolean allowed = user.getAuthorities().stream()
                .map(Authority::getId)
                .anyMatch(id -> AUTH_ID_PROFILO_SUPER.equals(id) || authorizedCodes.contains(id));

        if (!allowed) {
            throw new ActionNotPermittedException(
                    "User with user Id: " + userId + " doesn't have authority to do this action"
            );
        }
    }

}
