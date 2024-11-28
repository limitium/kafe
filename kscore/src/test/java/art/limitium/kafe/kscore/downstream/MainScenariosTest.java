package art.limitium.kafe.kscore.downstream;

import art.limitium.kafe.kscore.kstreamcore.audit.AuditWrapperSupplier;
import art.limitium.kafe.ksmodel.audit.Audit;
import art.limitium.kafe.ksmodel.downstream.Request;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static art.limitium.kafe.kscore.kstreamcore.downstream.DownstreamResendProcessor.RESEND_MODEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class MainScenariosTest extends BaseDSTest {

    @Test
    void testNewAmendCancelRequest() {
        //new
        send(SOURCE, 1, 1L);
        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew = parseOutput(waitForRecordFrom(SINK2));

        assertEquals("new", ds1outNew.requestType());
        assertEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        assertEquals("new", ds2outNew.requestType());
        assertEquals("1", ds2outNew.refId());
        assertEquals("1", ds2outNew.refVer());
        assertEquals("ds2", ds2outNew.dsId());
        assertEquals("rd2>1|1", ds2outNew.payload());

        //amend
        send(SOURCE, 1, 2L);

        Outgoing ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outAmend = parseOutput(waitForRecordFrom(SINK2));

        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals("1", ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>2|2", ds1outNew.payload());

        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("2", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>2|2", ds2outAmend.payload());
        //cancel
        send(SOURCE, 1, 0L);

        ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outCancel = parseOutput(waitForRecordFrom(SINK2));

        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals(ds1outNew.refId(), ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        assertEquals("cancel", ds2outCancel.requestType());
        assertEquals("1", ds2outCancel.refId());
        assertEquals("2", ds2outCancel.refVer());
        assertEquals("ds2", ds2outCancel.dsId());
        assertEquals("-", ds2outCancel.payload());
    }

    @Test
    void testReply() {
        send(SOURCE, 0, 1, 1L);
        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNew = parseOutput(waitForRecordFrom(SINK3));

        assertEquals("new", ds1outNew.requestType());
        assertEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        assertEquals("new", ds2outNew.requestType());
        assertEquals("1", ds2outNew.refId());
        assertEquals("1", ds2outNew.refVer());
        assertEquals("ds2", ds2outNew.dsId());
        assertEquals("rd2>1|1", ds2outNew.payload());

        assertEquals("new", ds3outNew.requestType());
        assertEquals("1", ds3outNew.refId());
        assertEquals("1", ds3outNew.refVer());
        assertEquals("ds3", ds3outNew.dsId());
        assertEquals("rd3>1|1", ds3outNew.payload());

        Request request1 = waitForRecordFrom(REQUESTS1_CL).value().value();
        Request request2 = waitForRecordFrom(REQUESTS2_CL).value().value();//pending
        request2 = waitForRecordFrom(REQUESTS2_CL).value().value();//autocommit
        Request request3 = waitForRecordFrom(REQUESTS3_CL).value().value();

        long request1Id = request1.id;
        assertEquals(Request.RequestType.NEW, request1.type);
        assertEquals(Request.RequestState.PENDING, request1.state);
        assertEquals(0, request1.respondedAt);

        assertEquals(Request.RequestType.NEW, request2.type);
        assertEquals(Request.RequestState.ACKED, request2.state);

        long request3Id = request3.id;
        assertEquals(Request.RequestType.NEW, request3.type);
        assertEquals(Request.RequestState.PENDING, request3.state);
        assertEquals(0, request3.respondedAt);


        send(REPLY1, 0, ds1outNew.correlationId(), "true,-,-");
        send(REPLY3, 0, ds3outNew.correlationId(), "false,123,bad");

        request1 = waitForRecordFrom(REQUESTS1_CL).value().value();
        request3 = waitForRecordFrom(REQUESTS3_CL).value().value();

        assertEquals(Request.RequestState.ACKED, request1.state);
        assertEquals(request1Id, request1.id);
        assertNotEquals(0, request1.respondedAt);

        assertEquals(Request.RequestState.NACKED, request3.state);
        assertEquals(request3Id, request3.id);
        assertEquals("123", request3.respondedCode);
        assertEquals("bad", request3.respondedMessage);
        assertNotEquals(0, request3.respondedAt);
    }

    @Test
    void testResend() {
        send(SOURCE, 0, 1, 1L);
        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNew = parseOutput(waitForRecordFrom(SINK3));

        assertEquals("new", ds1outNew.requestType());
        assertEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        assertEquals("new", ds2outNew.requestType());
        assertEquals("1", ds2outNew.refId());
        assertEquals("1", ds2outNew.refVer());
        assertEquals("ds2", ds2outNew.dsId());
        assertEquals("rd2>1|1", ds2outNew.payload());

        assertEquals("new", ds3outNew.requestType());
        assertEquals("1", ds3outNew.refId());
        assertEquals("1", ds3outNew.refVer());
        assertEquals("ds3", ds3outNew.dsId());
        assertEquals("rd3>1|1", ds3outNew.payload());

        Request request1 = waitForRecordFrom(REQUESTS1_CL).value().value();
        Request request2 = waitForRecordFrom(REQUESTS2_CL).value().value();//pending
        request2 = waitForRecordFrom(REQUESTS2_CL).value().value();//autocommit
        Request request3 = waitForRecordFrom(REQUESTS3_CL).value().value();

        long request1Id = request1.id;
        assertEquals(Request.RequestType.NEW, request1.type);
        assertEquals(Request.RequestState.PENDING, request1.state);
        assertEquals(0, request1.respondedAt);

        assertEquals(Request.RequestType.NEW, request2.type);
        assertEquals(Request.RequestState.ACKED, request2.state);

        long request3Id = request3.id;
        assertEquals(Request.RequestType.NEW, request3.type);
        assertEquals(Request.RequestState.PENDING, request3.state);
        assertEquals(0, request3.respondedAt);


        send(REPLY1, 0, ds1outNew.correlationId(), "true,-,-");
        send(REPLY3, 0, ds3outNew.correlationId(), "false,123,bad");

        request1 = waitForRecordFrom(REQUESTS1_CL).value().value();
        request3 = waitForRecordFrom(REQUESTS3_CL).value().value();

        assertEquals(Request.RequestState.ACKED, request1.state);
        assertEquals(request1Id, request1.id);
        assertNotEquals(0, request1.respondedAt);

        assertEquals(Request.RequestState.NACKED, request3.state);
        assertEquals(request3Id, request3.id);
        assertEquals("123", request3.respondedCode);
        assertEquals("bad", request3.respondedMessage);
        assertNotEquals(0, request3.respondedAt);


        send(RESEND1, 0, request1.referenceId, RESEND_MODEL);
        send(RESEND2, 0, request1.referenceId, RESEND_MODEL);
        send(RESEND3, 0, request1.referenceId, RESEND_MODEL);

        Outgoing ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals("1", ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        Outgoing ds2outAmend = parseOutput(waitForRecordFrom(SINK2));
        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("2", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>1|1", ds2outAmend.payload());

        Outgoing ds3outAmend = parseOutput(waitForRecordFrom(SINK3));
        assertEquals("new", ds3outAmend.requestType());
        assertEquals("1", ds3outAmend.refId());
        assertEquals("1", ds3outAmend.refVer());
        assertEquals("ds3", ds3outAmend.dsId());
        assertEquals("rd3>1|1", ds3outAmend.payload());
    }

    @Test
    void testTerminate() {
        send(SOURCE, 0, 1, 1L);

        Request request1 = waitForRecordFrom(REQUESTS1_CL).value().value();
        Request request2 = waitForRecordFrom(REQUESTS2_CL).value().value();//pending
        request2 = waitForRecordFrom(REQUESTS2_CL).value().value();//autocommit
        Request request3 = waitForRecordFrom(REQUESTS3_CL).value().value();

        assertEquals(Request.RequestType.NEW, request1.type);
        assertEquals(Request.RequestState.PENDING, request1.state);
        assertEquals(0, request1.respondedAt);

        assertEquals(Request.RequestType.NEW, request2.type);
        assertEquals(Request.RequestState.ACKED, request2.state);

        assertEquals(Request.RequestType.NEW, request3.type);
        assertEquals(Request.RequestState.PENDING, request3.state);
        assertEquals(0, request3.respondedAt);

        send(TERMINATE1, request1.referenceId, request1.id);
        send(TERMINATE2, request2.referenceId, request2.id);
        send(TERMINATE3, request3.referenceId, request3.id);

        request1 = waitForRecordFrom(REQUESTS1_CL).value().value();
        request2 = waitForRecordFrom(REQUESTS2_CL).value().value();
        request3 = waitForRecordFrom(REQUESTS3_CL).value().value();

        assertEquals(Request.RequestType.NEW, request1.type);
        assertEquals(Request.RequestState.TERMINATED, request1.state);

        assertEquals(Request.RequestType.NEW, request2.type);
        assertEquals(Request.RequestState.TERMINATED, request2.state);

        assertEquals(Request.RequestType.NEW, request3.type);
        assertEquals(Request.RequestState.TERMINATED, request3.state);
    }

    @Test
    void testOverride() {
        send(SOURCE, 0, 1, 1L);

        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNew = parseOutput(waitForRecordFrom(SINK3));

        assertEquals("new", ds1outNew.requestType());
        assertEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1", ds1outNew.payload());

        assertEquals("new", ds2outNew.requestType());
        assertEquals("1", ds2outNew.refId());
        assertEquals("1", ds2outNew.refVer());
        assertEquals("ds2", ds2outNew.dsId());
        assertEquals("rd2>1|1", ds2outNew.payload());

        assertEquals("new", ds3outNew.requestType());
        assertEquals("1", ds3outNew.refId());
        assertEquals("1", ds3outNew.refVer());
        assertEquals("ds3", ds3outNew.dsId());
        assertEquals("rd3>1|1", ds3outNew.payload());

        send(OVERRIDE1, 0, Long.parseLong(ds1outNew.refId()), "111");
        send(OVERRIDE2, 0, Long.parseLong(ds2outNew.refId()), "222");
        send(OVERRIDE3, 0, Long.parseLong(ds3outNew.refId()), "333");

        Outgoing ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals("1", ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1+111", ds1outNew.payload());

        Outgoing ds2outAmend = parseOutput(waitForRecordFrom(SINK2));
        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("2", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>1|1+222", ds2outAmend.payload());

        Outgoing ds3outAmend = parseOutput(waitForRecordFrom(SINK3));
        assertEquals("amend", ds3outAmend.requestType());
        assertEquals("1", ds3outAmend.refId());
        assertEquals("2", ds3outAmend.refVer());
        assertEquals("ds3", ds3outAmend.dsId());
        assertEquals("333", ds3outAmend.payload());
    }

    @Test
    void testForceAck() {
        send(SOURCE, 0, 1, 1L);

        Request request1 = waitForRecordFrom(REQUESTS1_CL).value().value();
        Request request2 = waitForRecordFrom(REQUESTS2_CL).value().value();//pending
        request2 = waitForRecordFrom(REQUESTS2_CL).value().value();//autocommit
        Request request3 = waitForRecordFrom(REQUESTS3_CL).value().value();

        assertEquals(Request.RequestType.NEW, request1.type);
        assertEquals(Request.RequestState.PENDING, request1.state);
        assertEquals(0, request1.respondedAt);

        assertEquals(Request.RequestType.NEW, request2.type);
        assertEquals(Request.RequestState.ACKED, request2.state);

        assertEquals(Request.RequestType.NEW, request3.type);
        assertEquals(Request.RequestState.PENDING, request3.state);
        assertEquals(0, request3.respondedAt);

        send(FORCEACK1, 0, request1.correlationId, "");
        send(FORCEACK2, 0, request2.correlationId, "");
        send(FORCEACK3, 0, request3.correlationId, "");

        request1 = waitForRecordFrom(REQUESTS1_CL).value().value();
        request2 = waitForRecordFrom(REQUESTS2_CL).value().value();
        request3 = waitForRecordFrom(REQUESTS3_CL).value().value();

        assertEquals(Request.RequestType.NEW, request1.type);
        assertEquals(Request.RequestState.ACKED, request1.state);

        assertEquals(Request.RequestType.NEW, request2.type);
        assertEquals(Request.RequestState.ACKED, request2.state);

        assertEquals(Request.RequestType.NEW, request3.type);
        assertEquals(Request.RequestState.ACKED, request3.state);
    }

    @Test
    void testTraces() {
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(SOURCE.topic, null, SOURCE.keySerde.serializer().serialize(null, 10), SOURCE.valueSerde.serializer().serialize(null, 1L));
        producerRecord.headers()
                .add(AuditWrapperSupplier.AuditHeaders.TRACE, "xxxx-123-xxx".getBytes(StandardCharsets.UTF_8))
                .add(AuditWrapperSupplier.AuditHeaders.USER, "aaa".getBytes(StandardCharsets.UTF_8))
                .add(AuditWrapperSupplier.AuditHeaders.REASON, "bbb".getBytes(StandardCharsets.UTF_8));
        send(producerRecord);

        Audit audit1r = waitForRecordFrom(REQUESTS1_CL).value().wrapper();
        Audit audit1o = waitForRecordFrom(ORIGINALS1_CL).value().wrapper();
        Audit audit2r = waitForRecordFrom(REQUESTS2_CL).value().wrapper();
        Audit audit2o = waitForRecordFrom(ORIGINALS2_CL).value().wrapper();
        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));

        assertEquals(123, audit1r.traceId());
        assertEquals(123, audit1o.traceId());
        assertEquals(123, audit2r.traceId());
        assertEquals(123, audit2o.traceId());


        ProducerRecord<byte[], byte[]> producerRecordOver = new ProducerRecord<>(OVERRIDE1.topic, null, OVERRIDE1.keySerde.serializer().serialize(null, Long.parseLong(ds1outNew.refId())), OVERRIDE1.valueSerde.serializer().serialize(null, "111"));
        producerRecordOver.headers()
                .add(AuditWrapperSupplier.AuditHeaders.TRACE, "xxxx-321-xxx".getBytes(StandardCharsets.UTF_8))
                .add(AuditWrapperSupplier.AuditHeaders.USER, "aaa".getBytes(StandardCharsets.UTF_8))
                .add(AuditWrapperSupplier.AuditHeaders.REASON, "bbb".getBytes(StandardCharsets.UTF_8));
        send(producerRecordOver);

        audit1r = waitForRecordFrom(REQUESTS1_CL).value().wrapper();
        Audit audit1ov = waitForRecordFrom(OVERRIDE1_CL).value().wrapper();
        assertEquals(321, audit1r.traceId());
        assertEquals(321, audit1ov.traceId());
    }
}
