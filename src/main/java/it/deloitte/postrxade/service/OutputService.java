package it.deloitte.postrxade.service;

import it.deloitte.postrxade.entity.Output;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.exception.NotFoundRecordException;

import java.io.IOException;
import java.util.List;

public interface OutputService {
    List<Output> generateOutputMerchant(Submission submission);

    void generateSubmissionOutputTxt(Long submissionId) throws NotFoundRecordException, IOException;

    void generateSubmissionOutputTxtAsync(Long submissionId);
}
