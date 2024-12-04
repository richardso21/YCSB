package site.ycsb;

import com.oath.halodb.*;

import java.util.*;
import java.util.stream.Collectors;
import java.io.*;

/**
 * Halo-DB(binary) client for YCSB framework.
 * 
 */
public class HaloDBClient extends site.ycsb.DB {

  private HaloDB haloDB;

    /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    HaloDBOptions options = new HaloDBOptions();
    try {
      this.haloDB = haloDB.open("file.db", options);
    } catch (HaloDBException e) {
      return;
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    try {
      this.haloDB.close();
    } catch (HaloDBException e) {
      return;
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String keyWithTable = table + ":" + key;

    byte[] value;
    try {
      value = haloDB.get(keyWithTable.getBytes());
    } catch (HaloDBException e) {
      return Status.NOT_FOUND;
    }

    Map<String, byte[]> deserializedValue;
    try {
      deserializedValue = deserializeMap(value);
    } catch (IOException | ClassNotFoundException e) {
      return Status.ERROR;
    }

    result.putAll(tupleConvertFilter(decodeMap(deserializedValue), fields));

    return Status.OK;
  }
  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
   * in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                              Vector<HashMap<String, ByteIterator>> result) {
    String keyWithTable = table + ":" + startkey;
    
    HaloDBIterator iterator;
    
    try {
      iterator = haloDB.newIterator();
    } catch (HaloDBException e) {
      return Status.ERROR;
    }

    boolean reachedStartKey = false;
    while (iterator.hasNext() && recordcount > 0) {
      com.oath.halodb.Record record = iterator.next();
      if (record.getKey() == startkey.getBytes()) {
        reachedStartKey = true;
      }
      byte[] value = record.getValue();

      Map<String, byte[]> deserializedValue;
      try {
        deserializedValue = deserializeMap(value);
      } catch (IOException | ClassNotFoundException e) {
        return Status.ERROR;
      }

      if (reachedStartKey) {
        result.add(tupleConvertFilter(decodeMap(deserializedValue), fields));
        recordcount -= 1;
      }
    }

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String keyWithTable = table + ":" + key;

    byte[] value;
    try {
      value = haloDB.get(keyWithTable.getBytes());
    } catch (HaloDBException e) {
      return Status.NOT_FOUND;
    }

    byte[] serializedValue;
    try {
      serializedValue = serializeMap(encodeMap(values));
    } catch (IOException e) {
      return Status.ERROR;
    }
    try {
      haloDB.put(keyWithTable.getBytes(), serializedValue);
    } catch (HaloDBException e) {
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String keyWithTable = table + ":" + key;
    
    byte[] serializedValue;
    try {
      serializedValue = serializeMap(encodeMap(values));
    } catch (IOException e) {
      return Status.ERROR;
    }
    try {
      haloDB.put(keyWithTable.getBytes(), serializedValue);
    } catch (HaloDBException e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  public Status delete(String table, String key) {
    String keyWithTable = table + ":" + key;
    try {
      haloDB.delete(keyWithTable.getBytes());
      return Status.OK;
    } catch (HaloDBException e) {
      return Status.ERROR;
    }
  }

  public static byte[] serializeMap(Map<String, byte[]> map) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(map);  // Serialize the map
      return byteArrayOutputStream.toByteArray();  // Return byte array
    }
  }

  public static Map<String, byte[]> deserializeMap(byte[] byteArray) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray);
         ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
      return (Map<String, byte[]>) objectInputStream.readObject();  // Deserialize the map
    }
  }

  private Map<String, byte[]> encodeMap(Map<String, ByteIterator> map) {
    return map.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,  // keep the original key
            entry -> entry.getValue().toArray()  // apply processValue to the value
        ));
  }

  private Map<String, ByteIterator> decodeMap(Map<String, byte[]> map) {
    return map.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,  // keep the original key
            entry -> new ByteArrayByteIterator(entry.getValue())  // apply processValue to the value
        ));
  }

  private HashMap<String, ByteIterator> tupleConvertFilter(Map<String, ByteIterator> input, Set<String> fields) {
    HashMap<String, ByteIterator> result = new HashMap<>();
    if (input == null) {
      return result;
    }
    for (String key: input.keySet()) {
      if (fields == null || fields.contains(key)) {
        result.put(key, input.get(key));
      }
    }
    return result;
  }

}
