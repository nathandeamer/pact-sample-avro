package com.example.avro.provider;

import au.com.dius.pact.core.model.ContentTypeHint;
import au.com.dius.pact.provider.MessageAndMetadata;
import au.com.dius.pact.provider.PactVerifyProvider;
import au.com.dius.pact.provider.junit5.MessageTestTarget;
import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junit5.PactVerificationInvocationContextProvider;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.loader.PactFolder;
import com.example.avro.Customer;
import com.example.avro.Order;
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
    private static final String CUSTOMER_ORDER_AVRO_CONTENT_TYPE = "avro/binary; record=Customer";
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
        Order order = new Order(100L, Status.CREATED);

        return new MessageAndMetadata(
                serialise(order),
                Map.of(
                        KEY_CONTENT_TYPE, ORDER_AVRO_CONTENT_TYPE,
                        KEY_CONTENT_TYPE_HINT, CONTENT_TYPE_HINT));
    }

    @PactVerifyProvider("Customer Created")
    public MessageAndMetadata customerCreatedEvent() throws IOException {
        Customer customer = new Customer(1L, "Harry", "Potter", "harry.potter@hogwarts.com");

        return new MessageAndMetadata(
                serialise(customer),
                Map.of(
                        KEY_CONTENT_TYPE, CUSTOMER_ORDER_AVRO_CONTENT_TYPE,
                        KEY_CONTENT_TYPE_HINT, CONTENT_TYPE_HINT));
    }

    private byte[] serialise(Order record) throws IOException {
        SpecificDatumWriter<Order> writer = new SpecificDatumWriter<>(Order.class);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            writer.write(record, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        }
    }

    private byte[] serialise(Customer record) throws IOException {
        SpecificDatumWriter<Customer> writer = new SpecificDatumWriter<>(Customer.class);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            writer.write(record, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        }
    }
}
