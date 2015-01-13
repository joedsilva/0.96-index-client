package ca.mcgill.distsys.hbase96.indexclient;

import ca.mcgill.distsys.hbase96.indexcommons.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommons.exceptions.IndexNotExistsException;
import ca.mcgill.distsys.hbase96.indexcommons.proto.ByteArrayCriterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion.CompareType;
import ca.mcgill.distsys.hbase96.indexcommons.proto.IndexedColumnQuery;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test {

  private static Log LOG = LogFactory.getLog(Test.class);
  
  public static boolean CLEAN = true;
  public static Class DATA_TYPE = String.class;
  public static String[] INDEX_TYPES = {"hybrid", "htable"};
  public static String INDEX_TYPE = INDEX_TYPES[0];

  public final String tableName = "test";
  public final byte[] family = Bytes.toBytes("cf");
  public final byte[] qualifierA = Bytes.toBytes("a");
  public final byte[] qualifierB = Bytes.toBytes("b");
  public final byte[] qualifierC = Bytes.toBytes("c");

  public final int numRows = 40;

  public final Column columnA = new Column(family, qualifierA);
  public final Column columnB = new Column(family, qualifierB);
  public final Column columnC = new Column(family, qualifierC);

  public final List<Column> columnsAB =
      Arrays.asList(new Column[] {columnA, columnB});
  public final List<Column> columnsBC =
      Arrays.asList(new Column[]{columnB, columnC});

  public Configuration conf;
  public HBaseAdmin admin;
  public HIndexedTable table;

  public static int n = 1;
  
  public void initialize() throws Throwable {
    conf = HBaseConfiguration.create();
    admin = new HBaseAdmin(conf);
    if (CLEAN) {
      createTable();
      openTable();
      createIndex();
      populateTable();
      //createIndex();
      scanTable();
    } else {
      openTable();
    }
  }

  public void finalize() throws Throwable {
    table.close();
  }

  /**
     * @param args
     * @throws Throwable
     * @throws ServiceException
     */
  public static void main(String[] args) {
    if (args.length >= 1) {
      LOG.info(args[0]);
      CLEAN = args[0].toLowerCase().equals("true");
    }
    if (args.length >= 2) {
      LOG.info(args[1]);
      if (Arrays.asList(INDEX_TYPES).contains(args[1].toLowerCase())) {
        INDEX_TYPE = args[1].toLowerCase();
      }
    }
    if (args.length >= 3) {
      LOG.info(args[2]);
      if (args[2].toLowerCase().equals("int")) {
        DATA_TYPE = Integer.class;
      }
    }
    Test test = new Test();
    try {
      test.initialize();
      test.run();
    } catch (Throwable t) {
      t.printStackTrace();
    }
    try {
      test.finalize();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public void run() throws Throwable {

    if (INDEX_TYPE.equals("htable")) {
      if (DATA_TYPE.equals(String.class)) {
        testGetByIndexQuery("value2");
        testGetByIndexQuery("xyz");
      }
      else {
        testGetByIndexQuery(2);
        testGetByIndexQuery(-1);
      }
    }

    else {
      if (DATA_TYPE.equals(String.class)) {
        testEqualsQuery("value2");
        testEqualsQuery("xyz");
        testGreaterThanQuery("value7");
        testGreaterThanQuery("xyz");
        testLessThanOrEqualsQuery("value3");
        testLessThanOrEqualsQuery("abc");
        testRangeQuery("value3", "value7");
        testRangeQuery("value7", "value3");
        testRangeQuery("value7", "z");
        testRangeQuery("abc", "def");
        testQueryProjectBC("value2");
        testQuerySelectAB("value0", "value0");
        testQuerySelectAB("value0", "xyz");
        testQuerySelectAB("xyz", "value0");
        testQuerySelectBC("value0", "value3");
        testQuerySelectBC("value0", "xyz");
        testQuerySelectBC("xyz", "value3");
        testQuerySelectAC("value2", "value0");
        testQuerySelectABProjectBC("value2", "value1");
      }

      else if (DATA_TYPE.equals(Integer.class)) {
        testEqualsQuery(2);
        testEqualsQuery(-1);
        testGreaterThanQuery(7);
        testGreaterThanQuery(-1);
        testLessThanOrEqualsQuery(3);
        testLessThanOrEqualsQuery(-2);
        testRangeQuery(3, 7);
        testRangeQuery(7, 3);
        testRangeQuery(7, -4);
        testRangeQuery(-2, -3);
        testQueryProjectBC(2);
        testQuerySelectAB(0, 0);
        testQuerySelectAB(0, -1);
        testQuerySelectAB(-1, 0);
        testQuerySelectBC(0, 3);
        testQuerySelectBC(0, -1);
        testQuerySelectBC(-1, 3);
        testQuerySelectAC(2, 0);
        testQuerySelectABProjectBC(2, 1);
      }
    }
  }

  public void dropTable() throws Throwable {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  public void createTable() throws Throwable {
    LOG.info("Creating table...");
    dropTable();
    HTableDescriptor td = new HTableDescriptor(tableName);
    HColumnDescriptor cd = new HColumnDescriptor(family);
    td.addFamily(cd);
    admin.createTable(td);
  }

  public void openTable() throws Throwable {
    table = new HIndexedTable(conf, tableName);
  }

  public void dropIndex() throws Throwable {
    try { table.deleteIndex(columnA);
    } catch (Exception ex) {}
    try { table.deleteIndex(columnsAB);
    } catch (Exception ex) {}
    try { table.deleteIndex(columnC);
    } catch (Exception ex) {}
  }

  public void createIndex() throws Throwable {
    LOG.info("Creating index...");
    dropIndex();
    if (INDEX_TYPE.equals("hybrid")) {
      table.createHybridIndex(columnA);
      table.createIndex(columnsAB);     // hashtable
      table.createHybridIndex(columnC);
    }
    else if (INDEX_TYPE.equals("htable")) {
      table.createHTableIndex(columnA);
      //table.createIndex(columnsAB);
      //table.createHTableIndex(columnC);
    }
  }

  public void populateTable() throws Throwable {
    LOG.info("Populating...");
    table.setAutoFlushTo(false);
    for (int i = 0; i < numRows; i++) {
      byte[] row, valueA, valueB, valueC;
      if (DATA_TYPE.equals(String.class)) {
        row = getBytes("row" + prefixZeroes("" + i));
        valueA = getBytes("value" + i % 10);
        valueB = getBytes("value" + i % 3);
        valueC = getBytes("value" + i % 6);
      }
      else {
        row = getBytes(i);
        valueA = getBytes(i % 10);
        valueB = getBytes(i % 3);
        valueC = getBytes(i % 6);
      }
      Put put = new Put(row);
      put.add(family, qualifierA, valueA);
      put.add(family, qualifierB, valueB);
      put.add(family, qualifierC, valueC);
      table.put(put);
    }
    table.flushCommits();
    table.setAutoFlushTo(true);
  }

  public void scanTable() throws Throwable {
    LOG.info("Scanning...");
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    while (true) {
      Result result = scanner.next();
      if (result == null) {
        break;
      }
      LOG.info(resultToString(result));
    }
  }

  public void testGetByIndexQuery(Object a) throws Throwable {
    LOG.info("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnA.toString() + " = " + a);
    //byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
    Result[] results = table.getBySecondaryIndex(
        columnA.getFamily(), columnA.getQualifier(), valueA);
    printResults(results);
  }

  public void testEqualsQuery(Object a) throws Throwable {
    LOG.info("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnA.toString() + " = " + a);
    //byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
    Criterion<?> criterion = new ByteArrayCriterion(columnA, valueA);
    IndexedColumnQuery query = new IndexedColumnQuery(criterion);
    List<Result> results = table.execIndexedQuery(query);
    printResults(results);
  }

  public void testGreaterThanQuery(Object a) throws Throwable {
    LOG.info("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnA.toString() + " > " + a);
    //byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
    Criterion<?> criterion =
        new ByteArrayCriterion(columnA, valueA, CompareType.GREATER);
    IndexedColumnQuery query = new IndexedColumnQuery(criterion);
    List<Result> results = table.execIndexedQuery(query);
    printResults(results);
  }

  public void testLessThanOrEqualsQuery(Object a) throws Throwable {
    LOG.info("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnA.toString() + " <= " + a);
    //byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
    Criterion<?> criterion =
        new ByteArrayCriterion(columnA, valueA, CompareType.LESS_OR_EQUAL);
    IndexedColumnQuery query = new IndexedColumnQuery(criterion);
    List<Result> results = table.execIndexedQuery(query);
    printResults(results);
  }

  public void testRangeQuery(Object a, Object b) throws Throwable {
    LOG.info("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + a + " <= " + columnA.toString() + " <= " + b);
    //byte[] valueA = Bytes.toBytes(a);
    //byte[] valueB = Bytes.toBytes(b);
    byte[] valueA = getBytes(a);
    byte[] valueB = getBytes(b);
    Criterion<?> criterion = new ByteArrayCriterion(columnA, valueA, valueB);
    IndexedColumnQuery query = new IndexedColumnQuery(criterion);
    List<Result> results = table.execIndexedQuery(query);
    printResults(results);
  }

  public void testQuerySelectAB(Object a, Object b)
  throws Throwable {
    LOG.info("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnA.toString() + " = " + a +
        " AND " + columnB.toString() + " = " + b);
    //byte[] valueA = Bytes.toBytes(a);
    //byte[] valueB = Bytes.toBytes(b);
    byte[] valueA = getBytes(a);
    byte[] valueB = getBytes(b);
    Criterion<?> criterionA = new ByteArrayCriterion(columnA, valueA);
    Criterion<?> criterionB = new ByteArrayCriterion(columnB, valueB);
    List<Criterion<?>> criteria =
        new ArrayList(Arrays.asList(criterionA, criterionB));
    IndexedColumnQuery query = new IndexedColumnQuery(criteria);
    List<Result> results = table.execIndexedQuery(query);
    printResults(results);
  }

  public void testQuerySelectBC(Object b, Object c)
  throws Throwable {
    LOG.info("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnB.toString() + " = " + b +
        " AND " + columnC.toString() + " = " + c);
    //byte[] valueB = Bytes.toBytes(b);
    //byte[] valueC = Bytes.toBytes(c);
    byte[] valueB = getBytes(b);
    byte[] valueC = getBytes(c);
    Criterion<?> criterionB = new ByteArrayCriterion(columnB, valueB);
    Criterion<?> criterionC = new ByteArrayCriterion(columnC, valueC);
    List<Criterion<?>> criteria =
        new ArrayList(Arrays.asList(criterionB, criterionC));
    IndexedColumnQuery query = new IndexedColumnQuery(criteria);
    List<Result> results = table.execIndexedQuery(query);
    printResults(results);
  }

  public void testQuerySelectAC(Object a, Object c)
      throws Throwable {
    LOG.info("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnA.toString() + " = " + a +
        " AND " + columnC.toString() + " = " + c);
    //byte[] valueA = Bytes.toBytes(a);
    //byte[] valueC = Bytes.toBytes(c);
    byte[] valueA = getBytes(a);
    byte[] valueC = getBytes(c);
    Criterion<?> criterionA = new ByteArrayCriterion(columnA, valueA);
    Criterion<?> criterionC = new ByteArrayCriterion(columnC, valueC);
    List<Criterion<?>> criteria =
        new ArrayList(Arrays.asList(criterionA, criterionC));
    IndexedColumnQuery query = new IndexedColumnQuery(criteria);
    List<Result> results = table.execIndexedQuery(query);
    printResults(results);
  }

  public void testQueryProjectBC(Object a) throws Throwable {
    LOG.info("Test " + n++ + ": " +
        "SELECT id, " + columnB.toString() + ", " + columnC.toString() +
        " FROM " + tableName +
        " WHERE " + columnA.toString() + " = " + a);
    //byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
    Criterion<?> criterion = new ByteArrayCriterion(columnA, valueA);
    List<Criterion<?>> criteria = new ArrayList(Arrays.asList(criterion));
    IndexedColumnQuery query = new IndexedColumnQuery(criteria, columnsBC);
    List<Result> results = table.execIndexedQuery(query);
    printResults(results);
  }

  public void testQuerySelectABProjectBC(Object a, Object b)
  throws Throwable {
    LOG.info("Test " + n++ + ": " + 
        "SELECT id, " + columnB.toString() + ", " + columnC.toString() +
            " WHERE " + columnA.toString() + " = " + a +
            " AND " + columnB.toString() + " = " + b);
    //byte[] valueA = Bytes.toBytes(a);
    //byte[] valueB = Bytes.toBytes(b);
    byte[] valueA = getBytes(a);
    byte[] valueB = getBytes(b);
    Criterion<?> criterionA = new ByteArrayCriterion(columnA, valueA);
    Criterion<?> criterionB = new ByteArrayCriterion(columnB, valueB);
    List<Criterion<?>> criteria =
        new ArrayList(Arrays.asList(criterionA, criterionB));
    IndexedColumnQuery query = new IndexedColumnQuery(criteria, columnsBC);
    List<Result> results = table.execIndexedQuery(query);
    printResults(results);
  }

  public void printResults(List<Result> results) {
    if (results == null) return;
    for (Result result : results) {
      LOG.info(resultToString(result));
    }
    LOG.info("");
  }

  public void printResults(Result[] results) {
    if (results == null) return;
    printResults(new ArrayList<Result>(Arrays.asList(results)));
  }

  public String resultToString(Result result) {
    String str = "";
    byte[] row = result.getRow();
    byte[] valueA = result.getValue(family, qualifierA);
    byte[] valueB = result.getValue(family, qualifierB);
    byte[] valueC = result.getValue(family, qualifierC);
    //str = str + (row != null ? Bytes.toInt(row) : "null") + ": ";
    //str = str + (valueA != null ? Bytes.toInt(valueA) : "null") + ", ";
    //str = str + (valueB != null ? Bytes.toInt(valueB) : "null") + ", ";
    //str = str + (valueC != null ? Bytes.toInt(valueC) : "null") + ", ";
    str = str + (row != null ? fromBytes(row) : "null") + ": ";
    str = str + (valueA != null ? fromBytes(valueA) : "null") + ", ";
    str = str + (valueB != null ? fromBytes(valueB) : "null") + ", ";
    str = str + (valueC != null ? fromBytes(valueC) : "null") + ", ";
    return str;
  }

  public String prefixZeroes(String row) {
    int maxDigits = ("" + numRows).length();
    int prefixDigits = maxDigits - row.length();
    for (int j = 0; j < prefixDigits; j++) row = "0" + row;
    return row;
  }

  public void testIntComparison() {
    int flagbit32 = (int) Math.pow(2, 31);   // 2^^31
    byte[] zero = getBytes(0);
    byte[] one = getBytes(1);
    byte[] _one = getBytes(-1);
    int intZero = fromBytes(zero);
    int intOne = fromBytes(one);
    int int_One = fromBytes(_one);
    LOG.info("" + intZero + " " + intOne + " " + int_One);
    LOG.info("0 , 0  " + Bytes.compareTo(zero, zero));
    LOG.info("0 , 1  " + Bytes.compareTo(zero, one));
    LOG.info("0 , -1  " + Bytes.compareTo(zero, _one));
    LOG.info("1 , 0  " + Bytes.compareTo(one, zero));
    LOG.info("1 , 1  " + Bytes.compareTo(one, one));
    LOG.info("1 , -1  " + Bytes.compareTo(one, _one));
    LOG.info("-1 , 0  " + Bytes.compareTo(_one, zero));
    LOG.info("-1 , 1  " + Bytes.compareTo(_one, one));
    LOG.info("-1 , -1  " + Bytes.compareTo(_one, _one));
  }

  public PositionedByteRange wrapper = new SimplePositionedByteRange();
  public byte[] getBytes(Object o) {
    if (o instanceof String) {
      String s = (String) o;
      OrderedBytes.encodeString(wrapper.set(s.getBytes().length+2), s, Order.ASCENDING);
    }
    else if (o instanceof Integer) {
      Integer i = (Integer) o;
      OrderedBytes.encodeInt32(wrapper.set(Integer.SIZE/4+1), i, Order.ASCENDING);
    }
    else if (o instanceof byte[]) {
      byte[] b = (byte[]) o;
      OrderedBytes.encodeBlobCopy(wrapper.set(b.length+1), b, Order.ASCENDING);
    }
    else {
      OrderedBytes.encodeNull(wrapper.set(1), Order.ASCENDING);  // Null
    }
    return wrapper.getBytes();
  }

  public <T> T fromBytes(byte[] b) {
    wrapper.set(b);
    if (OrderedBytes.isText(wrapper)) {
      return (T) (String) OrderedBytes.decodeString(wrapper);
    }
    else if (OrderedBytes.isFixedInt32(wrapper)) {
      return (T) (Integer) OrderedBytes.decodeInt32(wrapper);
    }
    else if (OrderedBytes.isBlobCopy(wrapper)) {
      return (T) (byte[]) OrderedBytes.decodeBlobCopy(wrapper);
    }
    else {
      return null;
    }
  }
}
