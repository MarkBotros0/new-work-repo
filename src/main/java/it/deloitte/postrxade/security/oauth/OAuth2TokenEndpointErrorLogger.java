package it.deloitte.postrxade.security.oauth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.security.oauth2.client.http.OAuth2ErrorResponseErrorHandler;
import org.springframework.web.client.ResponseErrorHandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Wraps {@link OAuth2ErrorResponseErrorHandler} per loggare status e body della risposta
 * quando il token endpoint OAuth2 (es. SSO Amex) restituisce errore, così da facilitare il debug
 * senza dover attivare log a livello di rete.
 */
public class OAuth2TokenEndpointErrorLogger implements ResponseErrorHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2TokenEndpointErrorLogger.class);

	private final OAuth2ErrorResponseErrorHandler delegate = new OAuth2ErrorResponseErrorHandler();

	@Override
	public boolean hasError(ClientHttpResponse response) throws IOException {
		return delegate.hasError(response);
	}

	@Override
	public void handleError(ClientHttpResponse response) throws IOException {
		int statusCode = response.getStatusCode().value();
		String body = readBody(response);
		LOGGER.error(
			"Token endpoint OAuth2 ha restituito errore: status={}, body={}",
			statusCode,
			body != null && !body.isEmpty() ? body : "(vuoto)"
		);
		// Delega al handler standard (che legge di nuovo il body per costruire OAuth2Error)
		delegate.handleError(new CachedBodyClientHttpResponse(response, body));
	}

	private static String readBody(ClientHttpResponse response) {
		try (InputStream is = response.getBody();
			 Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name()).useDelimiter("\\A")) {
			return scanner.hasNext() ? scanner.next() : "";
		} catch (Exception e) {
			LOGGER.warn("Impossibile leggere body risposta token endpoint: {}", e.getMessage());
			return "";
		}
	}

	/**
	 * Wrapper che espone lo stesso status/headers della risposta originale ma un body
	 * già letto (così il delegate può rileggerlo).
	 */
	private static class CachedBodyClientHttpResponse implements ClientHttpResponse {
		private final ClientHttpResponse delegate;
		private final String body;

		CachedBodyClientHttpResponse(ClientHttpResponse delegate, String body) {
			this.delegate = delegate;
			this.body = body != null ? body : "";
		}

		@Override
		public HttpStatusCode getStatusCode() throws IOException {
			return delegate.getStatusCode();
		}

		@Override
		public String getStatusText() throws IOException {
			return delegate.getStatusText();
		}

		@Override
		public void close() {
			delegate.close();
		}

		@Override
		public InputStream getBody() {
			return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
		}

		@Override
		public org.springframework.http.HttpHeaders getHeaders() {
			return delegate.getHeaders();
		}
	}
}
