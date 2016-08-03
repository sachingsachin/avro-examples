package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * Test avro for UUID support
 */
public class TestUuid extends BaseAvroTest {

  @Test
  public void testUUIDs() throws Exception {

    log("\n\n----- Creating entities -----");
    UuidTestA a = new UuidTestA();
    UuidTestB b = new UuidTestB();
    a.setB(b);
    b.setId(UUID.randomUUID());

    log("\n\n----- Testing serialization -----");
    byte[] bytes = serialize(a);

    log("\n\n----- Testing generic datum read -----");
    List<GenericRecord> genRecords =
      (List<GenericRecord>) genericDatumRead(bytes);
    GenericRecord genRecord = genRecords.get(0);
    assertNotNull ("Cannot read element 'b'", genRecord.get("b"));

    log("\n\n----- Testing reflect datum read -----");
    List<UuidTestA> appRecords =
      (List<UuidTestA>) reflectDatumRead(bytes, UuidTestA.class);
    UuidTestA appRecord = appRecords.get(0);
    log ("Read: " + appRecord);
    assertNotNull ("Unable to read app-record", appRecord);
    assertNotNull ("Unable to read app-record's child element", appRecord.getB());
    assertNotNull ("Unable to read ID", appRecord.getB().getId());
    assertTrue ("ID element should be of type UUID", appRecord.getB().getId() instanceof UUID);

    log("\n\n----- Testing JSON encoder/decoder -----");
    byte[] jsonBytes = encodeToJson (a);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    GenericRecord jsonRecord = decodeJson(jsonBytes, a);
    assertEquals ("JSON decoder output not same as Binary Decoder", genRecord, jsonRecord);
  }
}

class UuidTestA {

    UuidTestB b;

    public UuidTestB getB() {
        return b;
    }
    public void setB(UuidTestB b) {
        this.b = b;
    }
}

class UuidTestB {

    UUID id;

    public UUID getId() {
        return id;
    }
    public void setId(UUID id) {
        this.id = id;
    }
}

