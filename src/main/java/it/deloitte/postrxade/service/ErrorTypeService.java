package it.deloitte.postrxade.service;

import it.deloitte.postrxade.entity.ErrorType;
import it.deloitte.postrxade.exception.NotFoundRecordException;

public interface ErrorTypeService {
    ErrorType getErrorType(String errCode) throws NotFoundRecordException;
}
