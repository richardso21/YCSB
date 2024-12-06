package site.ycsb;


import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import jetbrains.exodus.util.ByteIterableUtil;

import java.io.*;

import java.util.*;
import java.util.stream.Collectors;

import static jetbrains.exodus.env.StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;

/**
 * Xodus client for YCSB framework.
 *
 */
public class XodusClient extends DB {

  private Environment environment;
  private Store store;

  @Override
  public void init() {
    environment = Environments.newInstance("/tmp/xodus-output-ycsb");
    store = environment.computeInTransaction(
        txn -> environment.openStore("MyStore", WITHOUT_DUPLICATES_WITH_PREFIXING, txn)
    );

  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Transaction txn = environment.beginReadonlyTransaction();
    String keyPrefix = table + ":" + key;
    ByteIterable keyByteIterable = StringBinding.stringToEntry(keyPrefix);


    ByteIterable byteIterable = store.get(txn, keyByteIterable);

    if (byteIterable == null) {
      return Status.NOT_FOUND;
    }

//    if (deserializeMap(byteIterable.getBytesUnsafe()) == null) {
//      return Status.ERROR;
//    }

    try {
      result.putAll(tupleConvertFilter(decodeMap(deserializeMap(byteIterable.getBytesUnsafe())), fields));
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    Transaction txn = environment.beginReadonlyTransaction();
    String keyPrefix = table + ":" + startkey;
    ByteIterable keyPrefixByteIterable = StringBinding.stringToEntry(keyPrefix);
    int prefixLen = keyPrefixByteIterable.getLength();

    try (Cursor cursor = store.openCursor(txn)) {
      if (cursor.getSearchKeyRange(keyPrefixByteIterable) != null) {
        do {
          ByteIterable key = cursor.getKey();
          // check if the key starts with keyPrefix
          int keyLen = key.getLength();
          if (keyLen < prefixLen ||
              ByteIterableUtil.compare(keyPrefixByteIterable, key.subIterable(0, prefixLen)) > 0) {
            break;
          }
          // wanted key/value pair is here
          ByteIterable value = cursor.getValue();
          Map<String, ByteIterator> retrievedFields = decodeMap(deserializeMap(value.getBytesUnsafe()));

          result.add(tupleConvertFilter(retrievedFields, fields));


        } while (cursor.getNext() && recordcount-- > 0);
      }
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Map<String, ByteIterator> results = new HashMap<>();

    this.read(table, key, null, results);
    if (results.size() == 0) {
      return Status.NOT_FOUND;
    }

    results.putAll(values);

    return this.insert(table, key, results);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    environment.executeInTransaction(txn -> {
        String keyPrefix = table + ":" + key;
        ByteIterable keyByteIterable = StringBinding.stringToEntry(keyPrefix);
        try {
          store.put(txn, keyByteIterable, new ArrayByteIterable(serializeMap(encodeMap(values))));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    // Start a transaction
    Transaction txn = environment.beginTransaction();

    // Construct the key by combining the table and the provided key
    String keyPrefix = table + ":" + key;
    ByteIterable keyByteIterable = StringBinding.stringToEntry(keyPrefix);

    // Check if the key exists in the store
    ByteIterable existingValue = store.get(txn, keyByteIterable);

    if (existingValue == null) {
      // If the key does not exist, return NOT_FOUND status
      return Status.NOT_FOUND;
    }

    // If the key exists, delete the entry
    store.delete(txn, keyByteIterable);

    // Commit the transaction and return success status
    txn.commit();
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
    for (String key : input.keySet()) {
      if (fields == null || fields.contains(key)) {
        result.put(key, input.get(key));
      }
    }
    return result;
  }
}