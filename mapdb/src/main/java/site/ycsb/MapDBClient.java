package site.ycsb;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * MapDB client for YCSB framework.
 *
 */
public class MapDBClient extends site.ycsb.DB {

  private DB mapDB;
  private BTreeMap<String, byte[]> db;

  @Override
  public void init() throws DBException {
    this.mapDB = DBMaker
        .fileDB("./file.db")
        .closeOnJvmShutdown()
        .make();

    db = mapDB.treeMap("db")
        .keySerializer(Serializer.JAVA)
        .valueSerializer(Serializer.BYTE_ARRAY)
        .createOrOpen();
  }

  public void cleanup() throws DBException {
    db.close();
    mapDB.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String searchKey = table + ":" + key;

    Map<String, ByteIterator> map;
    try {
      byte[] mapBytes = db.get(searchKey);
      if (mapBytes == null) {
        System.out.println("read " + table + " " +  key);
        return Status.NOT_FOUND;
      }
      map = decodeMap(deserializeMap(mapBytes));
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    if (fields == null) {
      result.putAll(map);
    } else {
      for (String field : fields) {
        result.put(field, map.get(field));
      }
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    Iterator<Map.Entry<String, byte[]>> cursor = db.tailMap(table + startkey).entrySet().iterator();

    while (cursor.hasNext() && recordcount-- > 0) {
      Map.Entry<String, byte[]> entry = cursor.next();
      try {
        result.add(tupleConvertFilter(decodeMap(deserializeMap(entry.getValue())), fields));
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String searchKey = table + ":" + key;

    Map<String, ByteIterator> map;
    try {
      byte[] mapBytes = db.get(searchKey);
      if (mapBytes == null) {
        return insert(table, key, values);
      } else {
        map = decodeMap(deserializeMap(mapBytes));
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    if (map == null) {
      return Status.NOT_FOUND;
    }

    map.putAll(values);

    try {
      db.put(searchKey, serializeMap(encodeMap(map)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String searchKey = table + ":" + key;

    try {
      db.put(searchKey, serializeMap(encodeMap(values)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    String searchKey = table + ":" + key;

    byte[] removed = db.remove(searchKey);

    if (removed == null) {
      return Status.NOT_FOUND;
    }
    return Status.OK;
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