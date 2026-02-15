package it.deloitte.postrxade.service.impl;

import it.deloitte.postrxade.dto.LogDTO;
import it.deloitte.postrxade.dto.AuditLogsSearchDTO;
import it.deloitte.postrxade.entity.Log;
import it.deloitte.postrxade.repository.LogRepository;
import it.deloitte.postrxade.service.AuditTrailService;
import it.deloitte.postrxade.utils.SortItem;
import it.deloitte.postrxade.utils.Utils;
import ma.glasnost.orika.MapperFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of the AuditTrailService.
 * <p>
 * This service handles the retrieval of system logs (Audit Trail).
 * It supports server-side pagination and sorting to efficiently handle large volumes of log data.
 * <p>
 * Note: While the search DTO may contain filtering criteria (like period or fiscal year),
 * the current implementation focuses on retrieving the full paginated list.
 */
@Service
@Transactional
public class AuditTrailServiceImpl implements AuditTrailService {

    @Autowired
    private @Qualifier("alternativeMapperFacade") MapperFacade alternativeMapperFacade;

    private final LogRepository logRepository;

    /**
     * Constructor-based dependency injection.
     *
     * @param logRepository The repository to access Log entities.
     */
    public AuditTrailServiceImpl(LogRepository logRepository) {
        this.logRepository = logRepository;
    }

    /**
     * Retrieves a paginated list of audit logs.
     * <p>
     * Logic:
     * 1. Extracts pagination (page number, size) and sorting details from the search DTO.
     * 2. Constructs a Spring Data {@link Pageable} object using a utility helper.
     * 3. Queries the database for the total count and the specific page of records.
     * 4. Maps the resulting {@link Log} entities to {@link LogDTO}s for the frontend.
     *
     * @param searchDTO The DTO containing pagination and sorting parameters.
     * @return A {@link Page} containing the requested {@link LogDTO}s.
     */
    @Override
    public Page<LogDTO> getAuditLogs(AuditLogsSearchDTO searchDTO) {
        // Extract pagination parameters
        Integer page = searchDTO.getPage();
        Integer size = searchDTO.getSize();
        List<SortItem> sortList = searchDTO.getSortList();

        // 1. Get Total Count (for pagination UI)
        long count = logRepository.count();

        // 2. Build Pageable Object
        Pageable pageable = Utils.createPageableBasedOnPageAndSizeAndSorting(sortList, page, size);

        // 3. Fetch Data
        Page<Log> recordsFromDb = logRepository.getAllLogsUsingPagination(pageable);

        // 4. Map to DTOs
        List<LogDTO> logDTOList = new ArrayList<>(
                alternativeMapperFacade.mapAsList(recordsFromDb.getContent(), LogDTO.class)
        );

        // 5. Return Page
        return new PageImpl<>(logDTOList, pageable, count);
    }
}
