package it.deloitte.postrxade.security;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Temporary mapping between Microsoft Entra ID role identifiers (GUID)
 * and the expected role format TENANT_APP_AMBIENTE_RUOLO.
 * <p>
 * Used only for the <strong>oidc-deloitte</strong> (Deloitte SSO / Entra ID) flow, which sends role GUIDs.
 * The <strong>oidc</strong> flow (Nexi, Amex) already sends roles in format
 * TENANT_APP_AMBIENTE_RUOLO; those values are returned unchanged (no mapping).
 * <p>
 * When Entra sends roles in the expected format, this mapping can be removed.
 */
public final class EntraRoleMapping {

	private static final Pattern GUID_PATTERN = Pattern.compile(
		"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
	);

	private static final Map<String, String> ENTRA_ID_TO_ROLE = Map.of(
		"8047cff8-d588-46f3-8bd4-75c66bb9d60d", "AMEX_POSAPP_STG_SPR",
		"f82b0254-73b2-47ee-83b9-5c5eff421f93", "NEXI_POSAPP_STG_SPR"
	);

	private EntraRoleMapping() {}

	/**
	 * Resolves an Entra ID role identifier (GUID) to TENANT_APP_AMBIENTE_RUOLO.
	 * Only applied when the value looks like a GUID (oidc-deloitte flow). For oidc (Nexi)
	 * the value is already in the expected format and is returned unchanged.
	 *
	 * @param value claim value from token (GUID from Entra, or TENANT_APP_AMBIENTE_RUOLO from Nexi)
	 * @return mapped role string for Entra GUIDs, or the original value
	 */
	public static String resolve(String value) {
		if (value == null || value.isBlank()) {
			return value;
		}
		String normalized = value.trim();
		// Only map values that look like Entra role IDs (GUID). Nexi/oidc sends full role names.
		if (!GUID_PATTERN.matcher(normalized).matches()) {
			return normalized;
		}
		String mapped = ENTRA_ID_TO_ROLE.get(normalized.toLowerCase());
		return mapped != null ? mapped : normalized;
	}

	/** For tests / debugging: known Entra role IDs. */
	public static Set<String> getKnownEntraIds() {
		return Collections.unmodifiableSet(ENTRA_ID_TO_ROLE.keySet());
	}
}
