package com.example.avro.provider;

import au.com.dius.pact.core.model.*;
import au.com.dius.pact.provider.MessageAndMetadata;
import au.com.dius.pact.provider.PactVerifyProvider;
import au.com.dius.pact.provider.junit5.MessageTestTarget;
import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junit5.PactVerificationInvocationContextProvider;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.loader.PactFolder;
import com.example.avro.Order;
import com.example.avro.OrderV2;
import com.example.avro.Status;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

@Provider("sample-avro-provider")
@PactFolder("build/pacts") //Remember to run the consumer tests first.
public class ProviderAvroPactTest {

    private static final String ORDER_AVRO_CONTENT_TYPE = "avro/binary; record=Order";
    private static final String KEY_CONTENT_TYPE = "contentType";
    private static final String KEY_CONTENT_TYPE_HINT = "contentTypeHint";
    private static final ContentTypeHint CONTENT_TYPE_HINT = ContentTypeHint.BINARY;

    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void testTemplate(PactVerificationContext context) {
        context.verifyInteraction();
    }

    @SuppressWarnings("JUnitMalformedDeclaration")
    @BeforeEach
    void setupTest(PactVerificationContext context) {
        context.setTarget(new MessageTestTarget());
    }

    @PactVerifyProvider("Order Created")
    public MessageAndMetadata orderCreatedEvent() throws IOException {
        OrderV2 order = new OrderV2(Status.CREATED, "nathan");

        return new MessageAndMetadata(
                serialise(order),
                Map.of(
                        KEY_CONTENT_TYPE, ORDER_AVRO_CONTENT_TYPE,
                        KEY_CONTENT_TYPE_HINT, CONTENT_TYPE_HINT));
    }

    private byte[] serialise(OrderV2 record) throws IOException {
        SpecificDatumWriter<OrderV2> writer = new SpecificDatumWriter<>(OrderV2.class);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            writer.write(record, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        }
    }

}
