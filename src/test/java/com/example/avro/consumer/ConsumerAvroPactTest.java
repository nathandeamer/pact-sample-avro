package com.example.avro.consumer;

import au.com.dius.pact.consumer.dsl.PactBuilder;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.consumer.junit5.ProviderType;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.V4Interaction;
import au.com.dius.pact.core.model.V4Pact;
import au.com.dius.pact.core.model.annotations.Pact;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
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
        return builder
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
    }

    @Test
    @PactTestFor(pactMethod = "customerConfigureRecordWithDependantRecord")
    void customerConsumerRecordWithDependantRecord(V4Interaction.AsynchronousMessage message) {
        assertTrue(true);
    }

    @Pact(consumer = "sample-avro-consumer")
    V4Pact orderConfigureRecordWithDependantRecord(PactBuilder builder) {
        return builder
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
    }

    @Test
    @PactTestFor(pactMethod = "orderConfigureRecordWithDependantRecord")
    void orderConsumerRecordWithDependantRecord(V4Interaction.AsynchronousMessage message) {
        assertTrue(true);
    }

}
