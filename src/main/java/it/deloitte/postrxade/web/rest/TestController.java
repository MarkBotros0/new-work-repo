package it.deloitte.postrxade.web.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for health checks and basic connectivity testing.
 * <p>
 * This controller provides simple endpoints to verify that the backend is running,
 * reachable, and able to serialize JSON responses correctly. It is useful for
 * DevOps health probes (e.g., Kubernetes readiness) and initial integration testing.
 */
@RestController
@RequestMapping("/api")
@CrossOrigin("*")
@Tag(name = "System Utility Service", description = "Endpoints for connectivity testing and health checks")
public class TestController {

    /**
     * Simple text-based connectivity check.
     * <p>
     * Endpoint: GET /api/test
     *
     * @return A plain string confirming the server is running and the current server time.
     */
    @GetMapping("/test")
    @Operation(summary = "Basic connectivity test", description = "Returns a simple string to verify the server is reachable")
    public String test() {
        return "Backend Spring Boot funzionante! Porta: 8082 - " + LocalDateTime.now();
    }

    /**
     * JSON-based health check.
     * <p>
     * Endpoint: GET /api/health
     * <p>
     * Used by monitoring tools to verify application uptime and basic status.
     *
     * @return A {@link ResponseEntity} containing system status details.
     */
    @GetMapping("/health")
    @Operation(summary = "System Health Check", description = "Returns system status, uptime, and timestamp")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("uptime", System.currentTimeMillis());
        response.put("timestamp", LocalDateTime.now().toString());
        response.put("application", "pos-trx-ade");
        return ResponseEntity.ok(response);
    }

    /**
     * Mock endpoint to test JSON array serialization.
     * <p>
     * Endpoint: GET /api/products
     *
     * @return A {@link ResponseEntity} containing a mock list of products.
     */
    @GetMapping("/products")
    @Operation(summary = "Mock Product List", description = "Returns a static list of products to test JSON array serialization")
    public ResponseEntity<Map<String, Object>> getProducts() {
        Map<String, Object> response = new HashMap<>();
        response.put("message", "Endpoint prodotti funzionante");
        response.put("products", new String[]{"Prodotto 1", "Prodotto 2", "Prodotto 3"});
        response.put("count", 3);
        return ResponseEntity.ok(response);
    }
}
