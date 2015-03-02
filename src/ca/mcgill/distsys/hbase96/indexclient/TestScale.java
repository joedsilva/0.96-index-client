package ca.mcgill.distsys.hbase96.indexclient;

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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

import javax.print.event.PrintEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;

public class TestScale {

  private static Log LOG = LogFactory.getLog(TestScale.class);
  
  public static boolean CLEAN = true;
  public static boolean INSERTS = true;
  public static boolean UPDATES = true;
  public static boolean READS = false;
  public static Class DATA_TYPE = String.class;
  public static String[] INDEX_TYPES =
      {"hybrid", "hybrid2", "hashtable", "htable", "none"};
  public static String INDEX_TYPE = INDEX_TYPES[0];

  public static final int numRows = 400000;
  public static final int numInserts = 100000;
  public static final int numUpdates = 100000;
  public static final int numGets = 1000;
  public static final int select = 5;

  public static final int numThreads = 12;
  public static final Thread[] threads = new Thread[numThreads];

  public final String tableName = "test";
  public final byte[] family = Bytes.toBytes("cf");
  public final byte[] qualifierA = Bytes.toBytes("a");
  public final byte[] qualifierB = Bytes.toBytes("b");
  public final byte[] qualifierC = Bytes.toBytes("c");

  public final Column columnA = new Column(family, qualifierA);
  public final Column columnB = new Column(family, qualifierB);
  public final Column columnC = new Column(family, qualifierC);

  public final List<Column> columnsAB =
      Arrays.asList(new Column[] {columnA, columnB});
  public final List<Column> columnsBC =
      Arrays.asList(new Column[] {columnB, columnC});

  public Configuration conf;
  public HBaseAdmin admin;
  public HIndexedTable table;

  public int n = 1;
  
  public void initialize(boolean clean) throws Throwable {
    conf = HBaseConfiguration.create();
    admin = new HBaseAdmin(conf);
    if (clean) {
      createTable();
      openTable();
      createIndex();
    } else {
      openTable();
    }
  }

  public void finalize() throws Throwable {
    table.close();
    admin.close();
  }

  /**
     * @param args
     * @throws Throwable
     * @throws ServiceException
     */
  public static void main(String[] args) {

    if (args.length >= 1) {
      //LOG.debug(args[0]);
      CLEAN = args[0].toLowerCase().equals("true");
    }
    if (args.length >= 2) {
      //LOG.debug(args[1]);
      if (Arrays.asList(INDEX_TYPES).contains(args[1].toLowerCase())) {
        INDEX_TYPE = args[1].toLowerCase();
      } else {
        LOG.warn("Index type " + args[1] + "unknown. " +
            "Using " + INDEX_TYPES[0] + " by default.");
      }
    }
    if (args.length >= 3) {
      //LOG.debug(args[2]);
      if (args[2].toLowerCase().equals("int")) {
        DATA_TYPE = Integer.class;
      }
    }

    LOG.info("Clean: " + CLEAN);
    LOG.info("Index Type: " + INDEX_TYPE);
    LOG.info("Data Type: " + DATA_TYPE);

    // Clean
    if (CLEAN) {
      // Initialize
      TestScale test = new TestScale();
      try {
        test.initialize(true);
      } catch (Throwable t) {
        t.printStackTrace();
      }
      try {
        test.finalize();
      } catch (Throwable t) {
        t.printStackTrace();
      }
      // Populate
      waitForThreads();
      final AtomicInteger startRow = new AtomicInteger(0);
      for (int t = 0; t < numThreads; t++) {
        threads[t] = new Thread(new Runnable() {
          @Override
          public void run() {
            int s = startRow.getAndAdd(numRows/numThreads);
            //LOG.debug("Started thread " + s);
            TestScale test = new TestScale();
            try {
              test.initialize(false);
              test.populateTable(s, numRows/numThreads);
            } catch (Throwable t) {
              t.printStackTrace();
            }
            try {
              test.finalize();
            } catch (Throwable t) {
              t.printStackTrace();
            }
          }
        });
        threads[t].start();
      }
      waitForThreads();
    }

    final AtomicInteger startRow = new AtomicInteger(0);

    if (INSERTS) {
      startRow.set(numRows);
      for (int t = 0; t < numThreads; t++) {
        threads[t] = new Thread(new Runnable() {
          @Override
          public void run() {
            int s = startRow.getAndAdd(numInserts / numThreads);
            //LOG.debug("Started thread " + s);
            TestScale test = new TestScale();
            try {
              test.initialize(false);
              test.populateTable(s, numInserts / numThreads);
            } catch (Throwable t) {
              t.printStackTrace();
            }
            try {
              test.finalize();
            } catch (Throwable t) {
              t.printStackTrace();
            }
          }
        });
        threads[t].start();
      }
      waitForThreads();
    }

    if (UPDATES) {
      p++;
      startRow.set(0);
      for (int t = 0; t < numThreads; t++) {
        threads[t] = new Thread(new Runnable() {
          @Override
          public void run() {
            int s = startRow.getAndAdd(numUpdates/numThreads);
            //LOG.debug("Started thread " + s);
            TestScale test = new TestScale();
            try {
              test.initialize(false);
              test.populateTable(s, numUpdates/numThreads);
            } catch (Throwable t) {
              t.printStackTrace();
            }
            try {
              test.finalize();
            } catch (Throwable t) {
              t.printStackTrace();
            }
          }
        });
        threads[t].start();
      }
      waitForThreads();
    }

    if (READS) {
      startRow.set(0);
      for (int t = 0; t < numThreads; t++) {
        threads[t] = new Thread(new Runnable() {
          @Override
          public void run() {
            int s = startRow.getAndAdd((numRows / select) / numThreads);
            TestScale test = new TestScale();
            try {
              test.initialize(false);
              test.run(s);
            } catch (Throwable t) {
              t.printStackTrace();
            }
            try {
              test.finalize();
            } catch (Throwable t) {
              t.printStackTrace();
            }
          }
        });
        threads[t].start();
      }
      waitForThreads();/**/
    }
  }

  public void run(int startRow) throws Throwable {
    LOG.info("Running tests...");

    // Mini warmup
    if (INDEX_TYPE.equals("none")) {
      testGetWithoutIndexQuery("value" + prefixZeroes("" + 0));
    }
    else if (INDEX_TYPE.equals("htable")) {
      testGetByIndexQuery("value" + prefixZeroes("" + 0));
    } else {
      testEqualsQuery("value" + prefixZeroes("" + 0));
    }

    long startTime = System.nanoTime();
    for (int i = startRow, j = 0; j < numGets/numThreads; i++, j++) {
      try {
        if (INDEX_TYPE.equals("none")) {
          if (DATA_TYPE.equals(String.class)) {
            testGetWithoutIndexQuery(
              "value" + prefixZeroes("" + (i % (numRows / select))));
          } else {
            testGetWithoutIndexQuery(i % (numRows / select));
          }
        }
        else if (INDEX_TYPE.equals("htable")) {
          if (DATA_TYPE.equals(String.class)) {
            testGetByIndexQuery(
                "value" + prefixZeroes("" + (i % (numRows / select))));
            //testGetByIndexQuery("xyz");
          } else {
            testGetByIndexQuery(i % (numRows / select));
            //testGetByIndexQuery(-1);
          }
        }
        else {
          if (DATA_TYPE.equals(String.class)) {
            testEqualsQuery(
                "value" + prefixZeroes("" + (i % (numRows / select))));
            //testEqualsQuery("xyz");
          } else if (DATA_TYPE.equals(Integer.class)) {
            testEqualsQuery(i % (numRows / select));
            //testEqualsQuery(-1);
          }
        }
      } catch (Throwable ex) {
        LOG.warn("Get error", ex);
      }
    }
    long duration = (System.nanoTime() - startTime) / 1000;
    LOG.info("Avg get = " + duration / (numGets/numThreads) + " us");
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
      //table.createIndex(columnsAB);     // hashtable
      //table.createHybridIndex(columnC);
    } else if (INDEX_TYPE.equals("hybrid2")) {
      table.createHybridIndex2(columnA);
      //table.createIndex(columnsAB);     // hashtable
      //table.createHybridIndex2(columnC);
    } else if (INDEX_TYPE.equals("hashtable")) {
      table.createHashTableIndex(columnA);
      //table.createIndex(columnsAB);
      //table.createHTableIndex(columnC);
    }
    else if (INDEX_TYPE.equals("htable")) {
      table.createHTableIndex(columnA);
      //table.createIndex(columnsAB);
      //table.createHTableIndex(columnC);
    }
    else if (INDEX_TYPE.equals("none")) {
      // Do nothing
    }
  }

  public static int p = 0;  // adder
  public void populateTable(int startRow, int numRows_) throws Throwable {
    LOG.info("Populating...");
    table.setAutoFlushTo(true);
    long totalDuration = 0;
    for (int i = startRow; i < startRow + numRows_; i++) {
      byte[] row, valueA, valueB, valueC;
      if (DATA_TYPE.equals(String.class)) {
        row = getBytes("row" + prefixZeroes("" + i));
        valueA = getBytes("value" + prefixZeroes("" +
            (i+p) % (numRows / select)));
        valueB = getBytes("value" + prefixZeroes("" + i));
        valueC = getBytes("value" + prefixZeroes("" + i));
      } else {
        row = getBytes(i);
        valueA = getBytes((i+p) % (numRows / select));
        valueB = getBytes(i);
        valueC = getBytes(i);
      }
      LOG.trace("put: " + "row" + prefixZeroes("" + i) + ": " +
          "value" + prefixZeroes("" + (i + p) % (numRows / select)));
      Put put = new Put(row);
      put.add(family, qualifierA, valueA);
      put.add(family, qualifierB, valueB);
      put.add(family, qualifierC, valueC);
      long startTime = System.nanoTime();
      try {
        table.put(put);
      } catch (IOException ex) {
        LOG.warn("Put error", ex);
      }
      long duration = (System.nanoTime() - startTime) / 1000;
      //LOG.debug("put: " + duration + " us");
      if (numRows_ < 5000 || i > 5000) {
        totalDuration += duration;
      }
    }
    try {
      table.flushCommits();
    } catch (IOException ex) {
      LOG.warn("Commit error", ex);
    }
    LOG.info("Avg insert = " + totalDuration / numRows_ + " us");
    table.setAutoFlushTo(false);
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

  public void testGetWithoutIndexQuery(Object a) throws Throwable {
    LOG.trace("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnA.toString() + " = " + a);
    //byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
    Filter filter = new SingleColumnValueFilter(
        columnA.getFamily(), columnA.getQualifier(),
        CompareOp.EQUAL, valueA);
    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    Result[] results = scanner.next(2000);
    //printResults(results);
  }

  public void testGetByIndexQuery(Object a) throws Throwable {
    LOG.trace("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnA.toString() + " = " + a);
    //byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
    Result[] results = table.getBySecondaryIndex(
        columnA.getFamily(), columnA.getQualifier(), valueA);
    //printResults(results);
  }

  public void testEqualsQuery(Object a) throws Throwable {
    LOG.trace("Test " + n++ + ": SELECT * FROM " + tableName +
        " WHERE " + columnA.toString() + " = " + a);
    //byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
    Criterion<?> criterion = new ByteArrayCriterion(columnA, valueA);
    IndexedColumnQuery query = new IndexedColumnQuery(criterion);
    List<Result> results = table.execIndexedQuery(query);
    //printResults(results);
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
    for (int i = 0; i < results.size(); i++) {
      LOG.info(resultToString(results.get(i)));
      if (i > 100) break;
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

  public byte[] getBytes(Object o) {
    PositionedByteRange wrapper = new SimplePositionedByteRange();
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
    PositionedByteRange wrapper = new SimplePositionedByteRange();
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

  public static void waitForThreads() {
    for (int t = 0; t < numThreads; t++) {
      if (threads[t] != null) {
        try {
          threads[t].join();
        } catch (InterruptedException ex) {}
        threads[t] = null;
      }
    }
    try { Thread.sleep(5000); }
    catch (InterruptedException ex) {}
  }
}
