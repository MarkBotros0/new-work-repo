package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.entity.ErrorType;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.repository.ErrorTypeRepository;
import it.deloitte.postrxade.service.ErrorTypeService;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class ErrorTypeServiceImpl implements ErrorTypeService {

    private final ErrorTypeRepository repo;
    private Map<String, ErrorType> errorCache;

    public ErrorTypeServiceImpl(ErrorTypeRepository repo) {
        this.repo = repo;
    }

    @PostConstruct
    void init() {
        Map<String, ErrorType> temp = new HashMap<>();
        repo.findAll().forEach(et -> temp.put(et.getErrorCode(), et));
        errorCache = Map.copyOf(temp);
    }

    @Override
    public ErrorType getErrorType(String errCode) throws NotFoundRecordException {
        ErrorType type = errorCache.get(errCode);
        if (type == null) {
            throw new NotFoundRecordException("Error type with error code " + errCode + " not found");
        }
        return type;
    }
}


