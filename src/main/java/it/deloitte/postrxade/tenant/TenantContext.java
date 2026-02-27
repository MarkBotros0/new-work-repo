package it.deloitte.postrxade.tenant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-local context for storing the current tenant identifier.
 * This ensures that each request thread has its own tenant context,
 * preventing tenant data leakage between concurrent requests.
 */
public class TenantContext {

    /** Session attribute where the tenant id (resolved from SSO token at login) is stored. */
    public static final String SESSION_ATTRIBUTE_TENANT_ID = "SESSION_TENANT_ID";

    private static final Logger LOGGER = LoggerFactory.getLogger(TenantContext.class);
    private static final ThreadLocal<String> TENANT_ID = new ThreadLocal<>();
    
    /**
     * Sets the current tenant identifier for this thread.
     * 
     * @param tenantId the tenant identifier (nexi=Nexi, amex=Amex)
     */
    public static void setTenantId(String tenantId) {
        LOGGER.debug("Setting tenant ID: {}", tenantId);
        TENANT_ID.set(tenantId);
    }
    
    /**
     * Gets the current tenant identifier for this thread.
     * 
     * @return the tenant identifier, or null if not set
     */
    public static String getTenantId() {
        String tenantId = TENANT_ID.get();
        LOGGER.debug("Getting tenant ID: {}", tenantId);
        return tenantId;
    }
    
    /**
     * Clears the tenant context for this thread.
     * Should be called after request processing to prevent memory leaks.
     */
    public static void clear() {
        LOGGER.debug("Clearing tenant context");
        TENANT_ID.remove();
    }
    
    /**
     * Checks if a tenant is currently set in the context.
     * 
     * @return true if a tenant is set, false otherwise
     */
    public static boolean hasTenant() {
        return TENANT_ID.get() != null;
    }
}
