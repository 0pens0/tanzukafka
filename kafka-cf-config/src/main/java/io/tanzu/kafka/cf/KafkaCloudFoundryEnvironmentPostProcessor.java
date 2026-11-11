package io.tanzu.kafka.cf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
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
        // Explicit overrides win (used when OSB bindings are stale / broker unreachable).
        String overrideBootstrap = System.getenv("TANZU_KAFKA_BOOTSTRAP_SERVERS");
        String overrideUsername = System.getenv("TANZU_KAFKA_USERNAME");
        String overridePassword = System.getenv("TANZU_KAFKA_PASSWORD");
        boolean useOverride = overrideBootstrap != null && !overrideBootstrap.trim().isEmpty()
                && overrideUsername != null && !overrideUsername.trim().isEmpty()
                && overridePassword != null && !overridePassword.trim().isEmpty();

        String vcapServices = System.getenv("VCAP_SERVICES");
        if (!useOverride && (vcapServices == null || vcapServices.trim().isEmpty())) {
            return;
        }

        try {
            JsonNode credentials = null;
            if (!useOverride) {
                credentials = findKafkaCredentials(vcapServices);
                if (credentials == null) {
                    return;
                }
            }

            String brokers = useOverride
                    ? overrideBootstrap.trim()
                    : credentials.path("bootstrap_servers").get(0).asText();
            String securityProtocol = useOverride
                    ? firstNonBlank(System.getenv("TANZU_KAFKA_SECURITY_PROTOCOL"), "SASL_SSL")
                    : credentials.path("security_protocol").asText("SASL_SSL");
            boolean tlsEnabled = useOverride
                    || credentials.path("tls").path("enable").asBoolean(false)
                    || credentials.path("tls").path("enabled").asBoolean(false);
            // Tanzu Kafka TCP routes terminate TLS at the edge while advertising SASL_PLAINTEXT.
            if (tlsEnabled && "SASL_PLAINTEXT".equalsIgnoreCase(securityProtocol)) {
                securityProtocol = "SASL_SSL";
            }

            String mechanism = useOverride
                    ? firstNonBlank(System.getenv("TANZU_KAFKA_SASL_MECHANISM"), "SCRAM-SHA-512")
                    : credentials.path("sasl").path("mechanism").asText("SCRAM-SHA-512");
            String username = useOverride ? overrideUsername.trim() : credentials.path("sasl").path("username").asText();
            String password = useOverride ? overridePassword : credentials.path("sasl").path("password").asText();

            String jaasConfig = String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                    username,
                    password
            );

            Map<String, Object> properties = new HashMap<>();
            putBinderProps(properties, "spring.cloud.stream.kafka.binder", brokers, securityProtocol, mechanism, jaasConfig);
            putBinderProps(properties, "spring.cloud.stream.kafka.streams.binder", brokers, securityProtocol, mechanism, jaasConfig);

            if ("SASL_SSL".equalsIgnoreCase(securityProtocol) || "SSL".equalsIgnoreCase(securityProtocol)) {
                String caPem = loadServicesCaPem(credentials);
                if (caPem != null && !caPem.isEmpty()) {
                    // Kafka 2.0 (Spring Cloud Greenwich) does not support PEM truststores.
                    File truststore = writeJksTruststore(caPem);
                    putSslTrust(properties, "spring.cloud.stream.kafka.binder.configuration", truststore);
                    putSslTrust(properties, "spring.cloud.stream.kafka.streams.binder.configuration", truststore);
                }
            }

            environment.getPropertySources().addFirst(new MapPropertySource(PROPERTY_SOURCE, properties));
        } catch (Exception ignored) {
            // Fall back to local application properties when VCAP parsing fails.
        }
    }

    private static String firstNonBlank(String value, String fallback) {
        return value != null && !value.trim().isEmpty() ? value.trim() : fallback;
    }

    private static void putBinderProps(
            Map<String, Object> properties,
            String prefix,
            String brokers,
            String securityProtocol,
            String mechanism,
            String jaasConfig
    ) {
        properties.put(prefix + ".brokers", brokers);
        properties.put(prefix + ".configuration.security.protocol", securityProtocol);
        properties.put(prefix + ".configuration.sasl.mechanism", mechanism);
        properties.put(prefix + ".configuration.sasl.jaas.config", jaasConfig);
    }

    private static void putSslTrust(Map<String, Object> properties, String prefix, File truststore) {
        properties.put(prefix + ".ssl.truststore.location", truststore.getAbsolutePath());
        properties.put(prefix + ".ssl.truststore.password", "changeit");
        properties.put(prefix + ".ssl.truststore.type", "JKS");
        // Broker leaf cert is CN=kafka; TCP route hostname differs, so disable endpoint ID check.
        properties.put(prefix + ".ssl.endpoint.identification.algorithm", "");
    }

    private static File writeJksTruststore(String caPem) throws Exception {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        Certificate cert = factory.generateCertificate(new ByteArrayInputStream(caPem.getBytes(StandardCharsets.UTF_8)));
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setCertificateEntry("opsmgr-services-tls-ca", cert);
        File truststore = File.createTempFile("kafka-trust-", ".jks");
        truststore.deleteOnExit();
        try (FileOutputStream out = new FileOutputStream(truststore)) {
            keyStore.store(out, "changeit".toCharArray());
        }
        return truststore;
    }

    private String loadServicesCaPem(JsonNode credentials) throws Exception {
        if (credentials != null) {
            JsonNode tls = credentials.path("tls");
            if (tls.hasNonNull("ca") && !tls.get("ca").asText().isEmpty()) {
                return tls.get("ca").asText();
            }
            if (tls.hasNonNull("certificate") && !tls.get("certificate").asText().isEmpty()) {
                return tls.get("certificate").asText();
            }
        }
        ClassPathResource resource = new ClassPathResource("opsmgr-services-tls-ca.pem");
        if (!resource.exists()) {
            return null;
        }
        return StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
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
