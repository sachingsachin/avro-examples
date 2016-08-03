package examples;

import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

/**
 * Common methods for serializing and deserializing.
 * Uses Generic, Reflect and JSON encoders/decoders.
 */
public class BaseAvroTest {

  public ReflectData getReflectData() {
      ReflectData rdata = ReflectData.AllowNull.get();
      rdata.addLogicalTypeConversion(new Conversions.UUIDConversion());
      return rdata;
  }
  /**
   * Serializes the given {@code entityObjs} into {@code byte[]}
   */
  public <T> byte[] serialize(T ... entityObjs) throws Exception {

    T entityObj1 = entityObjs[0];
    ReflectData rdata = getReflectData();

    Schema schema = rdata.getSchema(entityObj1.getClass());
    assertNotNull("Unable to get schema", schema);
    log ("Schema: \n" + schema.toString(true));

    ReflectDatumWriter<T> datumWriter =
      new ReflectDatumWriter (entityObj1.getClass(), rdata);
    DataFileWriter<T> fileWriter = new DataFileWriter<T> (datumWriter);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    fileWriter.create(schema, baos);
    for (T entityObj : entityObjs) {
      fileWriter.append(entityObj);
    }
    fileWriter.close();

    byte[] bytes = baos.toByteArray();
    return bytes;
  }

  /**
   * Deserializes the given {@code byte[]} using {@link GenericDatumReader} and
   * returns a list of {@link GenericRecord} objects
   */
  public <T> List<GenericRecord> genericDatumRead (byte[] bytes) throws IOException {

    GenericDatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord> ();
    SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
    DataFileReader<GenericRecord> fileReader =
      new DataFileReader<GenericRecord>(avroInputStream, datumReader);

    Schema schema = fileReader.getSchema();
    assertNotNull("Unable to get schema", schema);
    GenericRecord record = null;
    List<GenericRecord> records = new ArrayList<GenericRecord> ();
    while (fileReader.hasNext()) {
      GenericRecord readRecord = fileReader.next(record);
      records.add (readRecord);
      log("Read " + readRecord);
    }
    fileReader.close();
    return records;
  }

 /**
  * Deserializes the given {@code byte[]} using {@link ReflectDatumReader} and
  * returns a list of application-level objects
  */
  public <T> List<T> reflectDatumRead (byte[] bytes, Class<T> clazz) throws IOException {

    ReflectDatumReader<T> datumReader = new ReflectDatumReader<T> ();
    SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
    DataFileReader<T> fileReader = new DataFileReader<T>(avroInputStream, datumReader);

    fileReader.getSchema();
    T record = null;
    List<T> records = new ArrayList<T> ();
    while (fileReader.hasNext()) {
      records.add (fileReader.next(record));
    }
    fileReader.close();
    return records;
  }

  /**
   * Serializes the given {@code entityObj} to {@code byte[]} using {@code json-encoder}
   */
  public <T> byte[] encodeToJson (T entityObj) throws IOException {

    ReflectData rdata = getReflectData();

    Schema schema = rdata.getSchema(entityObj.getClass());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().jsonEncoder(schema, os);
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<T>(schema, rdata);
    datumWriter.write(entityObj, encoder);
    encoder.flush();

    byte[] bytes = os.toByteArray();
    log ("JSON encoder output:\n" + new String(bytes));
    return bytes;
  }

  /**
   * Deserializes the given {@code byte[]} using {@code json-decoder} and
   * returns a list of {@link GenericRecord} objects
   */
  public <T> GenericRecord decodeJson
    (byte[] bytes, T entityObj) throws IOException {

    ReflectData rdata = getReflectData();

    Schema schema = rdata.getSchema(entityObj.getClass());
    GenericDatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord>(schema);

    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(bytes));
    GenericRecord r = datumReader.read(null, decoder);
    return r;
  }

  /**
   * Helps replace the long {@code System.out.println} with just {@code log} :)
   */
  public void log (String msg) {
    System.out.println (msg);
  }
}
