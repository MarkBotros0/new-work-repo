package it.deloitte.postrxade.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.BufferingClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class RestTemplateConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(RestTemplateConfig.class);

	@Value("${application.rest.tracing-request.enabled}")
	private boolean configTracingRequestEnabled;

	// @Autowired
	// CloseableHttpClient httpClient;

	@Bean
	public RestTemplate restTemplate(CloseableHttpClient httpClient) {
		LOGGER.info("Configuration tracing request enabled: {}", configTracingRequestEnabled);

		RestTemplate restTemplate = null;
		if (configTracingRequestEnabled) {
			ClientHttpRequestFactory factory = new BufferingClientHttpRequestFactory(clientHttpRequestFactory(httpClient));
			restTemplate = new RestTemplate(factory);

			List<ClientHttpRequestInterceptor> interceptors = restTemplate.getInterceptors();
			if (CollectionUtils.isEmpty(interceptors)) {
				interceptors = new ArrayList<>();
			}
			restTemplate.setInterceptors(interceptors);

			// restTemplate.getMessageConverters().add(0, mappingJacksonHttpMessageConverter());
		}
		else {
			restTemplate = new RestTemplate(clientHttpRequestFactory(httpClient));
		}

		// restTemplate.getMessageConverters().add(0, createMappingJacksonHttpMessageConverter());

		ObjectMapper objectMapper = new ObjectMapper();
		// configure your ObjectMapper here
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
		converter.setObjectMapper(objectMapper);
		restTemplate.getMessageConverters().add(0, converter);

		return restTemplate;
	}

	@Bean
	public HttpComponentsClientHttpRequestFactory clientHttpRequestFactory(CloseableHttpClient httpClient) {
		HttpComponentsClientHttpRequestFactory clientHttpRequestFactory = new HttpComponentsClientHttpRequestFactory();
		clientHttpRequestFactory.setHttpClient(httpClient);
		return clientHttpRequestFactory;
	}

}