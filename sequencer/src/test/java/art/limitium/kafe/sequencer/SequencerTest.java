package art.limitium.kafe.sequencer;

import art.limitium.kafe.sequencer.Sequencer;
import org.junit.jupiter.api.Test;

import static art.limitium.kafe.sequencer.Sequencer.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

class SequencerTest {

    @Test
    void basicTest() {
        Sequencer sequencer = new Sequencer(() -> EPOCH_RESET + 1, 1, 1);

        String bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000100000010000000000", bits);
        bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000100000010000000001", bits);
        bits = String.format("%064d", new BigInteger(Long.toBinaryString(sequencer.getNext())));
        assertEquals("0000000000000000000000000000000000000000100000100000010000000010", bits);
    }

    //todo: test overflow
    @Test
    void parseTest() {
        long currentTimeMillis = System.currentTimeMillis();
        String formattedTime = DateTimeFormatter.ofPattern(TIME_PATTERN).format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTimeMillis), ZONE_OFFSET));

        Sequencer sequencer = new Sequencer(() -> currentTimeMillis, 2, 3);
        assertEquals("{time:\"" + formattedTime + "\",namespace:\"2\",partition:\"3\", sequence:\"0\"}", Sequencer.parse(sequencer.getNext()));

        sequencer = new Sequencer(() -> currentTimeMillis, 1, 2);
        assertEquals("{time:\"" + formattedTime + "\",namespace:\"1\",partition:\"2\", sequence:\"0\"}", Sequencer.parse(sequencer.getNext()));
        assertEquals("{time:\"" + formattedTime + "\",namespace:\"1\",partition:\"2\", sequence:\"1\"}", Sequencer.parse(sequencer.getNext()));
    }
}
