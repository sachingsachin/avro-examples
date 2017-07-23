package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * Test for classes extending Maps
 */
public class TestClassesExtendingMaps extends BaseAvroTest {

    @Test
    public void testMapExtensions() throws Exception {
        log("\n\n----- Creating Map entities -----");
        Map2 map2 = new Map2();
        map2.put(10L, 20L);
        map2.setA(10);
        map2.setB("Foo");


        log("\n\n----- Testing serialization -----");
        byte[] bytes = serialize(map2);

        log("\n\n----- Testing generic datum read -----");
        List<GenericRecord> genRecords =
                (List<GenericRecord>) genericDatumRead(bytes);
        GenericRecord genRecord = genRecords.get(0);
        log ("Read " + genRecord);
        assertNotNull ("Cannot read element 'a'", genRecord.get("a"));
        assertNotNull ("Cannot read element 'b'", genRecord.get("b"));

        log("\n\n----- Testing reflect datum read -----");
        List<Map2> appRecords =
                (List<Map2>) reflectDatumRead(bytes, Map2.class);
        Map2 appRecord = appRecords.get(0);
        log ("Read: " + appRecord);
        assertNotNull ("Unable to read app-record", appRecord);

        log("\n\n----- Testing JSON encoder/decoder -----");
        byte[] jsonBytes = encodeToJson (map2);
        assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
        GenericRecord jsonRecord = decodeJson(jsonBytes, map2);
        log ("Read from JSON: " + genRecord);
        assertEquals ("JSON decoder output not same as Binary Decoder", genRecord, jsonRecord);
    }
}


class Map2 extends HashMap<Long, Long> {
    Integer a;
    String b;
    public Integer getA() {
        return a;
    }
    public void setA(Integer a) {
        this.a = a;
    }
    public String getB() {
        return b;
    }
    public void setB(String b) {
        this.b = b;
    }
}
