package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class InterfaceTypesTest extends BaseAvroTest {

  @Test
  public void testMaps () throws Exception {
      log("\n\n----- Creating entities -----");

      MapWrapper iw = new MapWrapper();
      iw.setMap(new HashMap<Integer, Integer>());
      iw.getMap().put(100, 200);

      log("\n\n----- Testing serialization -----");
      byte[] bytes = serialize(iw);

      log("\n\n----- Testing generic datum read -----");
      List<GenericRecord> genRecords =
        (List<GenericRecord>) genericDatumRead(bytes);
      GenericRecord genRecord = genRecords.get(0);
      assertNotNull ("Cannot read element 'map'", genRecord.get("map"));

      log("\n\n----- Testing reflect datum read -----");
      List<MapWrapper> appRecords =
        (List<MapWrapper>) reflectDatumRead(bytes, MapWrapper.class);
      MapWrapper appRecord = appRecords.get(0);
      log ("Read: " + appRecord);
      assertNotNull ("Unable to read root", appRecord);
      assertNotNull ("Unable to read map element", appRecord.getMap());

      testJsonEncoders(iw, genRecord);
  }

  @Test
  public void testLists () throws Exception {
      log("\n\n----- Creating entities -----");

      ListWrapper iw = new ListWrapper();
      iw.setList(new ArrayList<Integer>());
      iw.getList().add(100);

      log("\n\n----- Testing serialization -----");
      byte[] bytes = serialize(iw);

      log("\n\n----- Testing generic datum read -----");
      List<GenericRecord> genRecords =
        (List<GenericRecord>) genericDatumRead(bytes);
      GenericRecord genRecord = genRecords.get(0);
      assertNotNull ("Cannot read element 'list'", genRecord.get("list"));

      log("\n\n----- Testing reflect datum read -----");
      List<ListWrapper> appRecords =
        (List<ListWrapper>) reflectDatumRead(bytes, ListWrapper.class);
      ListWrapper appRecord = appRecords.get(0);
      log ("Read: " + appRecord);
      assertNotNull ("Unable to read root", appRecord);
      assertNotNull ("Unable to read list element", appRecord.getList());

      testJsonEncoders(iw, genRecord);
  }

  @Test
  public void testSets () throws Exception {
      log("\n\n----- Creating entities -----");

      SetWrapper iw = new SetWrapper();
      iw.setSet(new HashSet<Integer>());
      iw.getSet().add(100);

      log("\n\n----- Testing serialization -----");
      byte[] bytes = serialize(iw);

      log("\n\n----- Testing generic datum read -----");
      List<GenericRecord> genRecords =
        (List<GenericRecord>) genericDatumRead(bytes);
      GenericRecord genRecord = genRecords.get(0);
      assertNotNull ("Cannot read element 'set'", genRecord.get("set"));

      log("\n\n----- Testing reflect datum read -----");
      List<ListWrapper> appRecords =
        (List<ListWrapper>) reflectDatumRead(bytes, ListWrapper.class);
      ListWrapper appRecord = appRecords.get(0);
      log ("Read: " + appRecord);
      assertNotNull ("Unable to read root", appRecord);
      assertNotNull ("Unable to read set element", appRecord.getList());

      testJsonEncoders(iw, genRecord);
  }

  private void testJsonEncoders(Object iw, GenericRecord genRecord) throws IOException {
      log("\n\n----- Testing JSON encoder/decoder -----");
      byte[] jsonBytes = encodeToJson (iw);
      assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
      GenericRecord jsonRecord = decodeJson(jsonBytes, iw);
      assertEquals ("JSON decoder output not same as Binary Decoder", genRecord, jsonRecord);
      log(jsonRecord.toString());
  }

  /**
   * Helps replace the long {@code System.out.println} with just {@code log} :)
   */
  public void log (String msg) {
    System.out.println (msg);
  }
}

class MapWrapper {
    private Map<Integer, Integer> map;

    public Map<Integer, Integer> getMap() {
        return map;
    }

    public void setMap(Map<Integer, Integer> map) {
        this.map = map;
    }

    @Override
    public String toString() {
        return "MapWrapper [map=" + map + "]";
    }
}

class ListWrapper {
    private List<Integer> list;

    public List<Integer> getList() {
        return list;
    }

    public void setList(List<Integer> list) {
        this.list = list;
    }

    @Override
    public String toString() {
        return "ListWrapper [list=" + list + "]";
    }
}

class SetWrapper {
    private Set<Integer> set;

    public Set<Integer> getSet() {
        return set;
    }

    public void setSet(Set<Integer> set) {
        this.set = set;
    }

    @Override
    public String toString() {
        return "SetWrapper [set=" + set + "]";
    }
}