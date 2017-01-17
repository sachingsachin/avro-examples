package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class ByteArrayTest extends BaseAvroTest {

  @Test
  public void testInterfaces () throws Exception {
      log("\n\n----- Creating entities -----");

      ByteArrayWrapper root = new ByteArrayWrapper();
      root.setBa("Hello".getBytes());
      root.setIa(new int[]{1,2,3});
      root.setBoa(new boolean[]{true, false, true, false, true});

      System.out.println(getReflectData().getSchema(root.getClass()));

      log("\n\n----- Testing serialization -----");
      byte[] bytes = serialize(root);

      log("\n\n----- Testing generic datum read -----");
      List<GenericRecord> genRecords =
        (List<GenericRecord>) genericDatumRead(bytes);
      GenericRecord genRecord = genRecords.get(0);
      assertNotNull ("Cannot read element 'ba'", genRecord.get("ba"));

      log("\n\n----- Testing reflect datum read -----");
      List<ByteArrayWrapper> appRecords =
        (List<ByteArrayWrapper>) reflectDatumRead(bytes, ByteArrayWrapper.class);
      ByteArrayWrapper appRecord = appRecords.get(0);
      log ("Read: " + appRecord);
      assertNotNull ("Unable to read root", appRecord);
      assertNotNull ("Unable to read 'ba' element", appRecord.getBa());

      log("\n\n----- Testing JSON encoder/decoder -----");
      byte[] jsonBytes = encodeToJson (root);
      assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
      GenericRecord jsonRecord = decodeJson(jsonBytes, root);
      assertEquals ("JSON decoder output not same as Binary Decoder", genRecord, jsonRecord);
  }

  /**
   * Helps replace the long {@code System.out.println} with just {@code log} :)
   */
  public void log (String msg) {
    System.out.println (msg);
  }
}

class ByteArrayWrapper {
    private int[] ia;
    private byte[] ba;
    private boolean[] boa;

    public int[] getIa() {
        return ia;
    }
    public void setIa(int[] ia) {
        this.ia = ia;
    }
    public byte[] getBa() {
        return ba;
    }
    public void setBa(byte[] ba) {
        this.ba = ba;
    }
    public boolean[] getBoa() {
        return boa;
    }
    public void setBoa(boolean[] boa) {
        this.boa = boa;
    }
}