package art.limitium.kafe.kscore.downstream;

import art.limitium.kafe.kscore.kstreamcore.Topic;
import art.limitium.kafe.ksmodel.audit.Audit;
import art.limitium.kafe.ksmodel.downstream.Request;
import art.limitium.kafe.ksmodel.store.WrappedValue;
import art.limitium.kafe.ksmodel.store.WrapperValueSerde;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class EdgeScenariosTest extends BaseDSTest {

    private static final Topic<Long, WrappedValue<Audit,String>> INJECT = new Topic<>("core-app.store-downstream-ds1-request_data_originals-inject", Serdes.Long(), WrapperValueSerde.create(Audit.AuditSerde(), Serdes.String()));

    @Test
    void testMultipleRetries() {
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

        //reties 1
        send(RESEND1, 0, Long.parseLong(ds1outNew.refId()), "");
        send(RESEND2, 0, Long.parseLong(ds2outNew.refId()), "");
        send(RESEND3, 0, Long.parseLong(ds3outNew.refId()), "");

        WrappedValue<Audit, Request> request1a = waitForRecordFrom(REQUESTS1_CL).value();
        WrappedValue<Audit, Request> request2a = waitForRecordFrom(REQUESTS2_CL).value();
        WrappedValue<Audit, Request> request2aa = waitForRecordFrom(REQUESTS2_CL).value();
        WrappedValue<Audit, Request> request3a = waitForRecordFrom(REQUESTS3_CL).value();

        assertEquals(Request.RequestState.PENDING, request1a.value().state);
        assertEquals(2, request1a.wrapper().version());

        assertEquals(Request.RequestState.PENDING, request2a.value().state);
        assertEquals(Request.RequestState.ACKED, request2aa.value().state);
        assertEquals(4, request2aa.wrapper().version());

        assertEquals(Request.RequestState.PENDING, request3a.value().state);
        assertEquals(2, request3a.wrapper().version());

        Outgoing ds1outNew2 = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew2 = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNew2 = parseOutput(waitForRecordFrom(SINK3));

        assertEquals(ds1outNew.requestType(), ds1outNew2.requestType());
        assertEquals(ds1outNew.refId(), ds1outNew2.refId());
        assertEquals(ds1outNew.refVer(), ds1outNew2.refVer());
        assertEquals(ds1outNew.dsId(), ds1outNew2.dsId());
        assertEquals(ds1outNew.payload(), ds1outNew2.payload());

        assertEquals(ds2outNew.requestType(), ds2outNew2.requestType());
        assertEquals(ds2outNew.refId(), ds2outNew2.refId());
        assertEquals(ds2outNew.refVer(), ds2outNew2.refVer());
        assertEquals(ds2outNew.dsId(), ds2outNew2.dsId());
        assertEquals(ds2outNew.payload(), ds2outNew2.payload());

        assertEquals(ds3outNew.requestType(), ds3outNew2.requestType());
        assertEquals(ds3outNew.refId(), ds3outNew2.refId());
        assertEquals(ds3outNew.refVer(), ds3outNew2.refVer());
        assertEquals(ds3outNew.dsId(), ds3outNew2.dsId());
        assertEquals(ds3outNew.payload(), ds3outNew2.payload());

        //reties 2
        send(RESEND1, 0, Long.parseLong(ds1outNew.refId()), "");
        send(RESEND2, 0, Long.parseLong(ds2outNew.refId()), "");
        send(RESEND3, 0, Long.parseLong(ds3outNew.refId()), "");

        WrappedValue<Audit, Request> request1b = waitForRecordFrom(REQUESTS1_CL).value();
        WrappedValue<Audit, Request> request2b = waitForRecordFrom(REQUESTS2_CL).value();
        WrappedValue<Audit, Request> request2bb = waitForRecordFrom(REQUESTS2_CL).value();
        WrappedValue<Audit, Request> request3b = waitForRecordFrom(REQUESTS3_CL).value();

        assertEquals(Request.RequestState.PENDING, request1b.value().state);
        assertEquals(3, request1b.wrapper().version());

        assertEquals(Request.RequestState.PENDING, request2b.value().state);
        assertEquals(Request.RequestState.ACKED, request2bb.value().state);
        assertEquals(6, request2bb.wrapper().version());

        assertEquals(Request.RequestState.PENDING, request3b.value().state);
        assertEquals(3, request3b.wrapper().version());

        Outgoing ds1outNew3 = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNew3 = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNew3 = parseOutput(waitForRecordFrom(SINK3));

        assertEquals(ds1outNew.requestType(), ds1outNew3.requestType());
        assertEquals(ds1outNew.refId(), ds1outNew3.refId());
        assertEquals(ds1outNew.refVer(), ds1outNew3.refVer());
        assertEquals(ds1outNew.dsId(), ds1outNew3.dsId());
        assertEquals(ds1outNew.payload(), ds1outNew3.payload());

        assertEquals(ds2outNew.requestType(), ds2outNew3.requestType());
        assertEquals(ds2outNew.refId(), ds2outNew3.refId());
        assertEquals(ds2outNew.refVer(), ds2outNew3.refVer());
        assertEquals(ds2outNew.dsId(), ds2outNew3.dsId());
        assertEquals(ds2outNew.payload(), ds2outNew3.payload());

        assertEquals(ds3outNew.requestType(), ds3outNew3.requestType());
        assertEquals(ds3outNew.refId(), ds3outNew3.refId());
        assertEquals(ds3outNew.refVer(), ds3outNew3.refVer());
        assertEquals(ds3outNew.dsId(), ds3outNew3.dsId());
        assertEquals(ds3outNew.payload(), ds3outNew3.payload());

        //prely

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
    void testMultipleOverrides() {
        send(SOURCE, 0, 1, 1L);

        Outgoing ds1outNewOrig = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNewOrig = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNewOrig = parseOutput(waitForRecordFrom(SINK3));

        assertEquals("new", ds1outNewOrig.requestType());
        assertEquals("1", ds1outNewOrig.refId());
        assertEquals("1", ds1outNewOrig.refVer());
        assertEquals("ds1", ds1outNewOrig.dsId());
        assertEquals("rd1>1|1", ds1outNewOrig.payload());

        assertEquals("new", ds2outNewOrig.requestType());
        assertEquals("1", ds2outNewOrig.refId());
        assertEquals("1", ds2outNewOrig.refVer());
        assertEquals("ds2", ds2outNewOrig.dsId());
        assertEquals("rd2>1|1", ds2outNewOrig.payload());

        assertEquals("new", ds3outNewOrig.requestType());
        assertEquals("1", ds3outNewOrig.refId());
        assertEquals("1", ds3outNewOrig.refVer());
        assertEquals("ds3", ds3outNewOrig.dsId());
        assertEquals("rd3>1|1", ds3outNewOrig.payload());

        send(OVERRIDE1, 0, Long.parseLong(ds1outNewOrig.refId()), "111");
        send(OVERRIDE2, 0, Long.parseLong(ds2outNewOrig.refId()), "222");
        send(OVERRIDE3, 0, Long.parseLong(ds3outNewOrig.refId()), "333");

        Outgoing ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals("1", ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
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

        ConsumerRecord<Long, WrappedValue<Audit, String>> override11 = waitForRecordFrom(OVERRIDE1_CL);
        ConsumerRecord<Long, WrappedValue<Audit, String>> override21 = waitForRecordFrom(OVERRIDE2_CL);
        ConsumerRecord<Long, WrappedValue<Audit, String>> override31 = waitForRecordFrom(OVERRIDE3_CL);

        assertEquals(1, override11.value().wrapper().version());
        assertEquals("111", override11.value().value());

        assertEquals(1, override21.value().wrapper().version());
        assertEquals("222", override21.value().value());

        assertEquals(1, override31.value().wrapper().version());
        assertEquals("333", override31.value().value());

        send(OVERRIDE1, 0, Long.parseLong(ds1outNewOrig.refId()), "1111");
        send(OVERRIDE2, 0, Long.parseLong(ds2outNewOrig.refId()), "2222");
        send(OVERRIDE3, 0, Long.parseLong(ds3outNewOrig.refId()), "3333");

        ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertNotEquals("1", ds1outCancel.refId()); //generated
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>1|1+1111", ds1outNew.payload());

        ds2outAmend = parseOutput(waitForRecordFrom(SINK2));
        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("3", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>1|1+2222", ds2outAmend.payload());

        ds3outAmend = parseOutput(waitForRecordFrom(SINK3));
        assertEquals("amend", ds3outAmend.requestType());
        assertEquals("1", ds3outAmend.refId());
        assertEquals("3", ds3outAmend.refVer());
        assertEquals("ds3", ds3outAmend.dsId());
        assertEquals("3333", ds3outAmend.payload());


        ConsumerRecord<Long, WrappedValue<Audit, String>> override12 = waitForRecordFrom(OVERRIDE1_CL);
        ConsumerRecord<Long, WrappedValue<Audit, String>> override22 = waitForRecordFrom(OVERRIDE2_CL);
        ConsumerRecord<Long, WrappedValue<Audit, String>> override32 = waitForRecordFrom(OVERRIDE3_CL);

        assertEquals(2, override12.value().wrapper().version());
        assertEquals("1111", override12.value().value());

        assertEquals(2, override22.value().wrapper().version());
        assertEquals("2222", override22.value().value());

        assertEquals(2, override32.value().wrapper().version());
        assertEquals("3333", override32.value().value());

    }
    @Test
    void testInjectRequestOriginals() {
        send(SOURCE, 0, 1, 1L);

        Outgoing ds1outNewOrig = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNewOrig = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNewOrig = parseOutput(waitForRecordFrom(SINK3));

        assertEquals("new", ds1outNewOrig.requestType());
        assertEquals("1", ds1outNewOrig.refId());
        assertEquals("1", ds1outNewOrig.refVer());
        assertEquals("ds1", ds1outNewOrig.dsId());
        assertEquals("rd1>1|1", ds1outNewOrig.payload());

        assertEquals("new", ds2outNewOrig.requestType());
        assertEquals("1", ds2outNewOrig.refId());
        assertEquals("1", ds2outNewOrig.refVer());
        assertEquals("ds2", ds2outNewOrig.dsId());
        assertEquals("rd2>1|1", ds2outNewOrig.payload());

        assertEquals("new", ds3outNewOrig.requestType());
        assertEquals("1", ds3outNewOrig.refId());
        assertEquals("1", ds3outNewOrig.refVer());
        assertEquals("ds3", ds3outNewOrig.dsId());
        assertEquals("rd3>1|1", ds3outNewOrig.payload());

        ConsumerRecord<Long, WrappedValue<Audit, String>> original = waitForRecordFrom(ORIGINALS1_CL);

        assertEquals(1,original.value().wrapper().version());
        assertEquals(0,original.value().wrapper().partition());
        assertEquals("rd1>1|1",original.value().value());

        send(INJECT,1L,new WrappedValue<>(new Audit(3,3,0,123,321,"qqq","aaa",false), "ZZZ"));

        original = waitForRecordFrom(ORIGINALS1_CL);

        assertEquals(3,original.value().wrapper().traceId());
        assertEquals(3,original.value().wrapper().version());
        assertEquals(0,original.value().wrapper().partition());
        assertEquals(123,original.value().wrapper().createdAt());
        assertEquals("qqq",original.value().wrapper().modifiedBy());
        assertEquals("ZZZ",original.value().value());
    }

    @Test
    void testDeleteOverride() {
        send(SOURCE, 0, 1, 1L);

        Outgoing ds1outNewOrig = parseOutput(waitForRecordFrom(SINK1));
        Outgoing ds2outNewOrig = parseOutput(waitForRecordFrom(SINK2));
        Outgoing ds3outNewOrig = parseOutput(waitForRecordFrom(SINK3));

        assertEquals("new", ds1outNewOrig.requestType());
        assertEquals("1", ds1outNewOrig.refId());
        assertEquals("1", ds1outNewOrig.refVer());
        assertEquals("ds1", ds1outNewOrig.dsId());
        assertEquals("rd1>1|1", ds1outNewOrig.payload());

        assertEquals("new", ds2outNewOrig.requestType());
        assertEquals("1", ds2outNewOrig.refId());
        assertEquals("1", ds2outNewOrig.refVer());
        assertEquals("ds2", ds2outNewOrig.dsId());
        assertEquals("rd2>1|1", ds2outNewOrig.payload());

        assertEquals("new", ds3outNewOrig.requestType());
        assertEquals("1", ds3outNewOrig.refId());
        assertEquals("1", ds3outNewOrig.refVer());
        assertEquals("ds3", ds3outNewOrig.dsId());
        assertEquals("rd3>1|1", ds3outNewOrig.payload());

        send(OVERRIDE1, 0, Long.parseLong(ds1outNewOrig.refId()), "111");
        send(OVERRIDE2, 0, Long.parseLong(ds2outNewOrig.refId()), "222");
        send(OVERRIDE3, 0, Long.parseLong(ds3outNewOrig.refId()), "333");

        Outgoing ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertEquals("1", ds1outCancel.refId());
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        Outgoing ds1outNew = parseOutput(waitForRecordFrom(SINK1));
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

        ConsumerRecord<Long, WrappedValue<Audit, String>> override11 = waitForRecordFrom(OVERRIDE1_CL);
        ConsumerRecord<Long, WrappedValue<Audit, String>> override21 = waitForRecordFrom(OVERRIDE2_CL);
        ConsumerRecord<Long, WrappedValue<Audit, String>> override31 = waitForRecordFrom(OVERRIDE3_CL);

        assertEquals(1, override11.value().wrapper().version());
        assertEquals("111", override11.value().value());

        assertEquals(1, override21.value().wrapper().version());
        assertEquals("222", override21.value().value());

        assertEquals(1, override31.value().wrapper().version());
        assertEquals("333", override31.value().value());

        send(SOURCE, 0, 1, 2L);

        ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertNotEquals("1", ds1outCancel.refId()); //generated
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>2|2+111", ds1outNew.payload());

        ds2outAmend = parseOutput(waitForRecordFrom(SINK2));
        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("3", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>2|2+222", ds2outAmend.payload());

        ensureEmptyTopic(SINK3);

        send(OVERRIDE1, 0, Long.parseLong(ds1outNewOrig.refId()), null);
        send(OVERRIDE2, 0, Long.parseLong(ds2outNewOrig.refId()), null);
        send(OVERRIDE3, 0, Long.parseLong(ds3outNewOrig.refId()), null);

        ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertNotEquals("1", ds1outCancel.refId()); //generated
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>2|2", ds1outNew.payload());

        ds2outAmend = parseOutput(waitForRecordFrom(SINK2));
        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("4", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>2|2", ds2outAmend.payload());

        ds3outAmend = parseOutput(waitForRecordFrom(SINK3));
        assertEquals("amend", ds3outAmend.requestType());
        assertEquals("1", ds3outAmend.refId());
        assertEquals("3", ds3outAmend.refVer());
        assertEquals("ds3", ds3outAmend.dsId());
        assertEquals("rd3>2|2", ds3outAmend.payload());


        ConsumerRecord<Long, WrappedValue<Audit, String>> override12 = waitForRecordFrom(OVERRIDE1_CL);
        ConsumerRecord<Long, WrappedValue<Audit, String>> override22 = waitForRecordFrom(OVERRIDE2_CL);
        ConsumerRecord<Long, WrappedValue<Audit, String>> override32 = waitForRecordFrom(OVERRIDE3_CL);

        assertEquals(2, override12.value().wrapper().version());
        assertEquals("111", override11.value().value());
        assertTrue(override12.value().wrapper().removed());

        assertEquals(2, override22.value().wrapper().version());
        assertEquals("222", override22.value().value());
        assertTrue(override22.value().wrapper().removed());

        assertEquals(2, override32.value().wrapper().version());
        assertEquals("333", override32.value().value());
        assertTrue(override32.value().wrapper().removed());


        send(SOURCE, 0, 1, 3L);

        ds1outCancel = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("cancel", ds1outCancel.requestType());
        assertNotEquals("1", ds1outCancel.refId()); //generated
        assertEquals("1", ds1outCancel.refVer());
        assertEquals("ds1", ds1outCancel.dsId());
        assertEquals("-", ds1outCancel.payload());

        ds1outNew = parseOutput(waitForRecordFrom(SINK1));
        assertEquals("new", ds1outNew.requestType());
        assertNotEquals("1", ds1outNew.refId());
        assertEquals("1", ds1outNew.refVer());
        assertEquals("ds1", ds1outNew.dsId());
        assertEquals("rd1>3|3", ds1outNew.payload());

        ds2outAmend = parseOutput(waitForRecordFrom(SINK2));
        assertEquals("amend", ds2outAmend.requestType());
        assertEquals("1", ds2outAmend.refId());
        assertEquals("5", ds2outAmend.refVer());
        assertEquals("ds2", ds2outAmend.dsId());
        assertEquals("rd2>3|3", ds2outAmend.payload());

        ds3outAmend = parseOutput(waitForRecordFrom(SINK3));
        assertEquals("amend", ds3outAmend.requestType());
        assertEquals("1", ds3outAmend.refId());
        assertEquals("4", ds3outAmend.refVer());
        assertEquals("ds3", ds3outAmend.dsId());
        assertEquals("rd3>3|3", ds3outAmend.payload());
    }
}
