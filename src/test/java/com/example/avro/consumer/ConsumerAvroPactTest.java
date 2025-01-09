package com.example.avro.consumer;

import au.com.dius.pact.consumer.dsl.PactBuilder;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.consumer.junit5.ProviderType;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.V4Interaction;
import au.com.dius.pact.core.model.V4Pact;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.core.support.json.JsonValue;
import com.example.avro.Customer;
import com.example.avro.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(
        pactVersion = PactSpecVersion.V4,
        providerType = ProviderType.ASYNCH,
        providerName = "sample-avro-provider"
)
public class ConsumerAvroPactTest {

    private final String customerSchemasPath = Paths.get("src", "main", "avro", "Customer.avsc").toAbsolutePath().toString();
    private final String orderSchemasPath = Paths.get("src", "main", "avro", "Order.avsc").toAbsolutePath().toString();

    @Pact(consumer = "sample-avro-consumer")
    V4Pact customerConfigureRecordWithDependantRecord(PactBuilder builder) {
        V4Pact pact = builder
                .usingPlugin("avro")
                .expectsToReceive("Customer Created", "core/interaction/message")
                .with(
                        Map.of(
                                "message.contents",
                                Map.ofEntries(
                                        Map.entry("pact:avro", customerSchemasPath),
                                        Map.entry("pact:record-name", "Customer"),
                                        Map.entry("pact:content-type", "avro/binary"),
                                        Map.entry("id", "notEmpty('100')"),
                                        Map.entry("firstName", "notEmpty('name-1')"),
                                        Map.entry("lastName", "notEmpty('name-1')"),
                                        Map.entry("email", "notEmpty('test@example.com')")
                                )))
                .toPact();

        return updateAvroPluginConfiguration(pact, Customer.SCHEMA$.toString());
    }

    @Test
    @PactTestFor(pactMethod = "customerConfigureRecordWithDependantRecord")
    void customerConsumerRecordWithDependantRecord(V4Interaction.AsynchronousMessage message) {
        assertTrue(true);
    }

    @Pact(consumer = "sample-avro-consumer")
    V4Pact orderConfigureRecordWithDependantRecord(PactBuilder builder) {
        V4Pact pact = builder
                .usingPlugin("avro")
                .expectsToReceive("Order Created", "core/interaction/message")
                .with(
                        Map.of(
                                "message.contents",
                                Map.ofEntries(
                                        Map.entry("pact:avro", orderSchemasPath),
                                        Map.entry("pact:record-name", "Order"),
                                        Map.entry("pact:content-type", "avro/binary"),
                                        Map.entry("id", "notEmpty('100')"),
                                        Map.entry("status", "matching(equalTo, 'CREATED')"))))
                .toPact();

        return updateAvroPluginConfiguration(pact, Order.SCHEMA$.toString());
    }

    @Test
    @PactTestFor(pactMethod = "orderConfigureRecordWithDependantRecord")
    void orderConsumerRecordWithDependantRecord(V4Interaction.AsynchronousMessage message) {
        assertTrue(true);
    }

    private V4Pact updateAvroPluginConfiguration(V4Pact pact, String schemaAsString) {
        pact.getInteractions().forEach(interaction -> {
            Map<String, Map<String, JsonValue>> pluginCongifuration = interaction.asV4Interaction().getPluginConfiguration();
            if (pluginCongifuration.containsKey("avro")) {
                pluginCongifuration.get("avro").put("schemaValue", new JsonValue.StringValue(schemaAsString));
            }
        });
        return pact;
    }

}
