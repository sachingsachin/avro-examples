package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class ParameterizedTypesTest extends BaseAvroTest {

  @Test
  public void testParameterizedSchema () throws Exception {
      log("\n\n----- Creating entities -----");

      ParamsLeaf<Integer, Integer> leaf = new ParamsLeaf<Integer, Integer>();
      leaf.setBar(100);
      leaf.setBar(200);

      ParamsRoot root = new ParamsRoot();
      root.setLeaf(leaf);

      System.out.println(getReflectData().getSchema(leaf.getClass())); // Test fails here
      System.out.println(getReflectData().getSchema(root.getClass())); // It will fail here too

      log("\n\n----- Testing serialization -----");
      byte[] bytes = serialize(root);

      log("\n\n----- Testing generic datum read -----");
      List<GenericRecord> genRecords =
        (List<GenericRecord>) genericDatumRead(bytes);
      GenericRecord genRecord = genRecords.get(0);
      assertNotNull ("Cannot read element 'middle'", genRecord.get("middle"));

      log("\n\n----- Testing reflect datum read -----");
      List<ParamsRoot> appRecords =
        (List<ParamsRoot>) reflectDatumRead(bytes, ParamsRoot.class);
      ParamsRoot appRecord = appRecords.get(0);
      log ("Read: " + appRecord);
      assertNotNull ("Unable to read root", appRecord);
      assertNotNull ("Unable to read leaf element", appRecord.getLeaf());

      log("\n\n----- Testing JSON encoder/decoder -----");
      byte[] jsonBytes = encodeToJson (leaf);
      assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
      GenericRecord jsonRecord = decodeJson(jsonBytes, leaf);
      assertEquals ("JSON decoder output not same as Binary Decoder", genRecord, jsonRecord);
  }

  /**
   * Helps replace the long {@code System.out.println} with just {@code log} :)
   */
  public void log (String msg) {
    System.out.println (msg);
  }
}

class ParamsRoot {
    private ParamsLeaf<Integer, Integer> leaf;

    public ParamsLeaf<Integer, Integer> getLeaf() {
        return leaf;
    }
    public void setLeaf(ParamsLeaf<Integer, Integer> leaf) {
        this.leaf = leaf;
    }
}

class ParamsLeaf<P, Q> {
    private P foo;
    private Q bar;

    public P getFoo() {
        return foo;
    }
    public void setFoo(P foo) {
        this.foo = foo;
    }
    public Q getBar() {
        return bar;
    }
    public void setBar(Q bar) {
        this.bar = bar;
    }
}