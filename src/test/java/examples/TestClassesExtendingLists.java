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
 * Test for classes extending Lists and Maps
 */
public class TestClassesExtendingLists extends BaseAvroTest {

    @Test
    public void testListExtensions() throws Exception {
        log("\n\n----- Creating List entities -----");
        List2 list2 = new List2();
        list2.add(10L);
        list2.setA(10);
        list2.setB("Foo");

        log("\n\n----- Testing serialization -----");
        byte[] bytes = serialize(list2);

        log("\n\n----- Testing generic datum read -----");
        List<GenericRecord> genRecords =
                (List<GenericRecord>) genericDatumRead(bytes);
        GenericRecord genRecord = genRecords.get(0);
        log ("Read " + genRecord);
        assertNotNull ("Cannot read element 'a'", genRecord.get("a"));
        assertNotNull ("Cannot read element 'b'", genRecord.get("b"));

        log("\n\n----- Testing reflect datum read -----");
        List<List2> appRecords =
                (List<List2>) reflectDatumRead(bytes, List2.class);
        List2 appRecord = appRecords.get(0);
        log ("Read: " + appRecord);
        assertNotNull ("Unable to read app-record", appRecord);

        log("\n\n----- Testing JSON encoder/decoder -----");
        byte[] jsonBytes = encodeToJson (list2);
        assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
        GenericRecord jsonRecord = decodeJson(jsonBytes, list2);
        log ("Read from JSON: " + genRecord);
        assertEquals ("JSON decoder output not same as Binary Decoder", genRecord, jsonRecord);
    }
}


class List2 extends ArrayList<Long> {
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