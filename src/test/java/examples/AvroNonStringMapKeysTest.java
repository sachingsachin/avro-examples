package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.junit.Test;

public class AvroNonStringMapKeysTest extends BaseAvroTest {

  @Test
  public void testMapSchema () throws Exception {
      log("\n\n----- Creating entities -----");
      Key k = new Key();
      k.setA(100);
      Value v = new Value();
      v.setB(200);
      MapEntity m = new MapEntity();
      m.setMap(new HashMap<Key, Value>());
      m.getMap().put(k, v);

      log("\n\n----- Testing serialization -----");
      byte[] bytes = serialize(m);

      log("\n\n----- Testing generic datum read -----");
      List<GenericRecord> genRecords =
        (List<GenericRecord>) genericDatumRead(bytes);
      GenericRecord genRecord = genRecords.get(0);
      assertNotNull ("Cannot read element 'map' field", genRecord.get("map"));

      log("\n\n----- Testing reflect datum read -----");
      List<MapEntity> appRecords =
        (List<MapEntity>) reflectDatumRead(bytes, MapEntity.class);
      MapEntity appRecord = appRecords.get(0);
      log ("Read: " + appRecord);
      assertNotNull ("Unable to read app-record", appRecord);
      assertNotNull ("Unable to read app-record's child element", appRecord.getMap());
      assertTrue ("Unable to read ID", appRecord.getMap().size() > 0);

      log("\n\n----- Testing JSON encoder/decoder -----");
      byte[] jsonBytes = encodeToJson (m);
      assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
      GenericRecord jsonRecord = decodeJson(jsonBytes, m);
      assertEquals ("JSON decoder output not same as Binary Decoder", genRecord, jsonRecord);
  }

  /**
   * Helps replace the long {@code System.out.println} with just {@code log} :)
   */
  public void log (String msg) {
    System.out.println (msg);
  }
}

class MapEntity {
    private HashMap<Key, Value> map;

    public HashMap<Key, Value> getMap() {
        return map;
    }
    public void setMap(HashMap<Key, Value> map) {
        this.map = map;
    }
}

class Key {
    private Integer a;

    public Integer getA() {
        return a;
    }
    public void setA(Integer a) {
        this.a = a;
    }
}

class Value {
    private Integer b;

    public Integer getB() {
        return b;
    }
    public void setB(Integer b) {
        this.b = b;
    }
}