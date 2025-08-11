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

    private final String orderSchemasPath = Paths.get("src", "main", "avro", "Order.avsc").toAbsolutePath().toString();

    @Pact(consumer = "sample-avro-consumer")
    V4Pact configureRecordWithDependantRecord(PactBuilder builder) {
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
    @PactTestFor(pactMethod = "configureRecordWithDependantRecord")
    void consumerRecordWithDependantRecord(V4Interaction.AsynchronousMessage message) {
        assertTrue(true);
    }

}
