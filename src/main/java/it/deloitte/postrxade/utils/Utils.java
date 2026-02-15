package it.deloitte.postrxade.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * General utility class for common helper methods.
 * <p>
 * This class cannot be instantiated. It currently provides centralized logic
 * for converting frontend sorting/pagination parameters into Spring Data {@link Pageable} objects.
 */
@Slf4j
public class Utils {

    // Private constructor to prevent instantiation
    private Utils() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Creates a Spring Data {@link Pageable} object based on page number, size, and sorting criteria.
     * <p>
     * Handles null safety for all inputs:
     * <ul>
     * <li>Default Page: 0</li>
     * <li>Default Size: 10</li>
     * <li>Default Sort: Unsorted if list is null</li>
     * </ul>
     *
     * @param sortList List of {@link SortItem} defining fields and directions.
     * @param page     The zero-indexed page number.
     * @param size     The number of items per page.
     * @return A constructed {@link Pageable} instance.
     */
    public static Pageable createPageableBasedOnPageAndSizeAndSorting(List<SortItem> sortList, Integer page, Integer size) {

        List<Sort.Order> orders = new ArrayList<>();

        if (sortList != null) {
            // iterate the SortList to see based on which attributes we are going to Order By the results.
            for (SortItem sortValue : sortList) {
                // Ensure we handle potential nulls in the SortItem if necessary,
                // though usually the DTO validation handles that.
                if (sortValue.getDirection() != null && sortValue.getField() != null) {
                    orders.add(new Sort.Order(sortValue.getDirection(), sortValue.getField()));
                }
            }
        }

        return PageRequest.of(
                Optional.ofNullable(page).orElse(0),
                Optional.ofNullable(size).orElse(10),
                Sort.by(orders));
    }
}
