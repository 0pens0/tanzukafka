package io.tanzu.kafka.cf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Reads Tanzu for Apache Kafka credentials from VCAP_SERVICES and configures
 * Spring Cloud Stream Kafka binders for Cloud Foundry deployments.
 */
public class KafkaCloudFoundryEnvironmentPostProcessor implements EnvironmentPostProcessor {

    private static final String PROPERTY_SOURCE = "kafkaCloudFoundry";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String vcapServices = System.getenv("VCAP_SERVICES");
        if (vcapServices == null || vcapServices.trim().isEmpty()) {
            return;
        }

        try {
            JsonNode credentials = findKafkaCredentials(vcapServices);
            if (credentials == null) {
                return;
            }

            String brokers = credentials.path("bootstrap_servers").get(0).asText();
            String securityProtocol = credentials.path("security_protocol").asText("SASL_SSL");
            String mechanism = credentials.path("sasl").path("mechanism").asText("SCRAM-SHA-512");
            String username = credentials.path("sasl").path("username").asText();
            String password = credentials.path("sasl").path("password").asText();

            String jaasConfig = String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                    username,
                    password
            );

            Map<String, Object> properties = new HashMap<>();
            properties.put("spring.cloud.stream.kafka.binder.brokers", brokers);
            properties.put("spring.cloud.stream.kafka.binder.configuration.security.protocol", securityProtocol);
            properties.put("spring.cloud.stream.kafka.binder.configuration.sasl.mechanism", mechanism);
            properties.put("spring.cloud.stream.kafka.binder.configuration.sasl.jaas.config", jaasConfig);

            properties.put("spring.cloud.stream.kafka.streams.binder.brokers", brokers);
            properties.put("spring.cloud.stream.kafka.streams.binder.configuration.security.protocol", securityProtocol);
            properties.put("spring.cloud.stream.kafka.streams.binder.configuration.sasl.mechanism", mechanism);
            properties.put("spring.cloud.stream.kafka.streams.binder.configuration.sasl.jaas.config", jaasConfig);

            environment.getPropertySources().addFirst(new MapPropertySource(PROPERTY_SOURCE, properties));
        } catch (Exception ignored) {
            // Fall back to local application properties when VCAP parsing fails.
        }
    }

    private JsonNode findKafkaCredentials(String vcapServices) throws Exception {
        JsonNode root = objectMapper.readTree(vcapServices);
        Iterator<String> serviceNames = root.fieldNames();
        while (serviceNames.hasNext()) {
            String serviceName = serviceNames.next();
            if (!serviceName.toLowerCase().contains("kafka")) {
                continue;
            }
            for (JsonNode binding : root.get(serviceName)) {
                JsonNode credentials = binding.path("credentials");
                if (credentials.has("bootstrap_servers")) {
                    return credentials;
                }
            }
        }
        return null;
    }
}
