package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.dto.IngestionDTO;
import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.enums.IngestionStatusEnum;
import it.deloitte.postrxade.repository.IngestionRepository;
import it.deloitte.postrxade.repository.IngestionStatusRepository;
import it.deloitte.postrxade.service.IngestionService;
import ma.glasnost.orika.MapperFacade;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Service implementation for Ingestion operations.
 */
@Service
@Transactional
public class IngestionServiceImpl implements IngestionService {

    private static final String NOT_FOUND_MSG = "Ingestion not found with id: ";

    private final IngestionRepository ingestionRepository;
    private final IngestionStatusRepository ingestionStatusRepository;
    private final MapperFacade mapperFacade;

    public IngestionServiceImpl(IngestionRepository ingestionRepository,
                                IngestionStatusRepository ingestionStatusRepository,
                               @Qualifier("mapperFacade") MapperFacade mapperFacade) {
        this.ingestionRepository = ingestionRepository;
        this.ingestionStatusRepository = ingestionStatusRepository;
        this.mapperFacade = mapperFacade;
    }

    @Override
    @Transactional(readOnly = true)
    public List<IngestionDTO> getAllIngestionsOrderByDate() {
        List<Ingestion> ingestions = ingestionRepository.findAllByOrderByIngestedAtDesc();
        return mapperFacade.mapAsList(ingestions, IngestionDTO.class);
    }

    @Override
    @Transactional(readOnly = true)
    public List<IngestionDTO> getAllIngestionsOrderByDateAsc() {
        List<Ingestion> ingestions = ingestionRepository.findAllByOrderByIngestedAtAsc();
        return mapperFacade.mapAsList(ingestions, IngestionDTO.class);
    }

    @Override
    @Transactional(readOnly = true)
    public Page<IngestionDTO> getAllIngestions(Pageable pageable) {
        Page<Ingestion> ingestionPage = ingestionRepository.findAll(pageable);
        return ingestionPage.map(ingestion -> mapperFacade.map(ingestion, IngestionDTO.class));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<IngestionDTO> getIngestionById(Long id) {
        return ingestionRepository.findById(id)
                .map(ingestion -> mapperFacade.map(ingestion, IngestionDTO.class));
    }

    @Override
    @Transactional(readOnly = true)
    public List<IngestionDTO> getIngestionsBySubmissionId(Long submissionId) {
        List<Ingestion> ingestions = ingestionRepository.findBySubmission_Id(submissionId);
        return mapperFacade.mapAsList(ingestions, IngestionDTO.class);
    }

    @Override
    @Transactional(readOnly = true)
    public List<IngestionDTO> getIngestionsByType(Long ingestionTypeId) {
        List<Ingestion> ingestions = ingestionRepository.findByIngestionType_Id(ingestionTypeId);
        return mapperFacade.mapAsList(ingestions, IngestionDTO.class);
    }

    @Override
    @Transactional(readOnly = true)
    public List<IngestionDTO> getIngestionsByStatus(Long ingestionStatusId) {
        List<Ingestion> ingestions = ingestionRepository.findByIngestionStatus_Id(ingestionStatusId);
        return mapperFacade.mapAsList(ingestions, IngestionDTO.class);
    }

    @Override
    @Transactional(readOnly = true)
    public List<IngestionDTO> getIngestionsByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        List<Ingestion> ingestions = ingestionRepository.findByIngestedAtBetween(startDate, endDate);
        return mapperFacade.mapAsList(ingestions, IngestionDTO.class);
    }

    @Override
    public IngestionDTO createIngestion(IngestionDTO ingestionDTO) {
        Ingestion ingestion = mapperFacade.map(ingestionDTO, Ingestion.class);
        Ingestion savedIngestion = ingestionRepository.save(ingestion);
        return mapperFacade.map(savedIngestion, IngestionDTO.class);
    }

    @Override
    public IngestionDTO updateIngestion(Long id, IngestionDTO ingestionDTO) {
        Ingestion existingIngestion = ingestionRepository.findById(id)
                .orElseThrow(() -> new RuntimeException(NOT_FOUND_MSG + id));
        
        // Update fields
        existingIngestion.setIngestedAt(ingestionDTO.getIngestedAt());
        // Commented out as per new schema
        // existingIngestion.setTotalTransactions(ingestionDTO.getTotalTransactions());
        // existingIngestion.setTotalReportableTransactions(ingestionDTO.getTotalReportableTransactions());
        existingIngestion.setFullPath(ingestionDTO.getFullPath());
        
        // Update ingestionError if provided
        if (ingestionDTO.getIngestionError() != null) {
            existingIngestion.setIngestionError(mapperFacade.map(ingestionDTO.getIngestionError(), IngestionError.class));
        }
        
        Ingestion updatedIngestion = ingestionRepository.save(existingIngestion);
        return mapperFacade.map(updatedIngestion, IngestionDTO.class);
    }

    @Override
    public void deleteIngestion(Long id) {
        if (!ingestionRepository.existsById(id)) {
            throw new RuntimeException(NOT_FOUND_MSG + id);
        }
        ingestionRepository.deleteById(id);
    }

    @Override
    public Ingestion createIngestionBySubmission(Submission submission, IngestionType ingestionType) {
        IngestionStatus ingestionStatus = ingestionStatusRepository.findOneByName(
                IngestionStatusEnum.PROCESSING.getLabel()
        ).orElse(null);

        Ingestion ingestion = new Ingestion();
        ingestion.setSubmission(submission);
        ingestion.setIngestionType(ingestionType);
        ingestion.setIngestedAt(LocalDateTime.now());
        ingestion.setIngestionStatus(ingestionStatus);

        return ingestionRepository.save(ingestion);
    }

    @Override
    public void markAsSuccess(Ingestion ingestion) {
        IngestionStatus ingestionStatus = ingestionStatusRepository.findOneByName(
                IngestionStatusEnum.SUCCESS.getLabel()
        ).orElse(null);

        ingestionRepository.updateIngestionStatus(ingestion.getId(), ingestionStatus);
    }
}
