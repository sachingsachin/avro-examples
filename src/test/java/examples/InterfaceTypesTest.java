package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class InterfaceTypesTest extends BaseAvroTest {

  @Test
  public void testInterfaces () throws Exception {
      log("\n\n----- Creating entities -----");

      InterfaceWrapper iw = new InterfaceWrapper();
      iw.setMap(new HashMap<Integer, Integer>());
      iw.getMap().put(100, 200);

      System.out.println(getReflectData().getSchema(iw.getClass()));

      log("\n\n----- Testing serialization -----");
      byte[] bytes = serialize(iw);

      log("\n\n----- Testing generic datum read -----");
      List<GenericRecord> genRecords =
        (List<GenericRecord>) genericDatumRead(bytes);
      GenericRecord genRecord = genRecords.get(0);
      assertNotNull ("Cannot read element 'map'", genRecord.get("map"));

      log("\n\n----- Testing reflect datum read -----");
      List<InterfaceWrapper> appRecords =
        (List<InterfaceWrapper>) reflectDatumRead(bytes, InterfaceWrapper.class);
      InterfaceWrapper appRecord = appRecords.get(0);
      log ("Read: " + appRecord);
      assertNotNull ("Unable to read root", appRecord);
      assertNotNull ("Unable to read map element", appRecord.getMap());

      log("\n\n----- Testing JSON encoder/decoder -----");
      byte[] jsonBytes = encodeToJson (iw);
      assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
      GenericRecord jsonRecord = decodeJson(jsonBytes, iw);
      assertEquals ("JSON decoder output not same as Binary Decoder", genRecord, jsonRecord);
  }

  /**
   * Helps replace the long {@code System.out.println} with just {@code log} :)
   */
  public void log (String msg) {
    System.out.println (msg);
  }
}

class InterfaceWrapper {
    private Map<Integer, Integer> map;

    public Map<Integer, Integer> getMap() {
        return map;
    }

    public void setMap(Map<Integer, Integer> map) {
        this.map = map;
    }
}