package ca.mcgill.distsys.hbase96.inmemindexedclient;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.exceptions.IndexNotExistsException;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.ByteArrayCriterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion.CompareType;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.IndexedColumnQuery;
import com.google.protobuf.ServiceException;
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

public class TestInt {

	public static boolean CLEAN = true;

	public static final String tableName = "test";
	public static final byte[] family = Bytes.toBytes("cf");
	public static final byte[] qualifierA = Bytes.toBytes("a");
	public static final byte[] qualifierB = Bytes.toBytes("b");
	public static final byte[] qualifierC = Bytes.toBytes("c");

	public static final int numRows = 40;

	public static final Column columnA = new Column(family, qualifierA);
	public static final Column columnB = new Column(family, qualifierB);
	public static final Column columnC = new Column(family, qualifierC);

	public static final List<Column> columnsAB = new ArrayList<>();
	public static final List<Column> columnsBC = new ArrayList<>();

  static {
		columnsAB.add(columnA);
		columnsAB.add(columnB);
		columnsBC.add(columnB);
		columnsBC.add(columnC);
	}

	public static Configuration conf;
	public static HBaseAdmin admin;
	public static HIndexedTable table;


	public static void initialize() throws Throwable {
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

	/**
	 * @param args
	 * @throws Throwable
	 * @throws ServiceException
	 */
	public static void main(String[] args) {
		try {
			if (args.length == 1) {
				CLEAN = args[0].toLowerCase().equals("true");
			}
			initialize();
      System.out.println("Test 0");
      testIntComparison();
      System.out.println("Test 1");
			testEqualsQuery(2);
      System.out.println("Test 2");
      testEqualsQuery(-1);
      System.out.println("Test 3");
      testGreaterThanQuery(7);
      System.out.println("Test 4");
      testGreaterThanQuery(-1);
      System.out.println("Test 5");
      testLessThanOrEqualsQuery(3);
      System.out.println("Test 6");
      testLessThanOrEqualsQuery(-2);
      System.out.println("Test 7");
      testRangeQuery(3, 7);
      System.out.println("Test 8");
      testRangeQuery(7, 3);
      System.out.println("Test 9");
      testRangeQuery(7, -4);
      System.out.println("Test 10");
      testRangeQuery(-2, -3);
      System.out.println("Test 11");
      testQueryProjectBC(2);
      System.out.println("Test 12");
      testQuerySelectAB(0, 0);
      System.out.println("Test 13");
      testQuerySelectAB(0, -1);
      System.out.println("Test 14");
      testQuerySelectAB(-1, 0);
      System.out.println("Test 15");
      testQuerySelectBC(0, 3);
      System.out.println("Test 16");
      testQuerySelectBC(0, -1);
      System.out.println("Test 17");
      testQuerySelectBC(-1, 3);
      System.out.println("Test 18");
      testQuerySelectAC(2, 0);
      System.out.println("Test 19");
      testQuerySelectABProjectBC(2, 1);
			//testComplexQuery();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		try {
			table.close();
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public static void dropTable() throws Throwable {
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
	}

	public static void createTable() throws Throwable {
		System.out.println("Creating table...");
		dropTable();
		HTableDescriptor td = new HTableDescriptor(tableName);
		HColumnDescriptor cd = new HColumnDescriptor(family);
		td.addFamily(cd);
		admin.createTable(td);
	}

	public static void openTable() throws Throwable {
		table = new HIndexedTable(conf, tableName);
	}

	public static void dropIndex() throws Throwable {
		try {
			table.deleteIndex(columnA);
			table.deleteIndex(columnsAB);
			table.deleteIndex(columnC);
		} catch (IndexNotExistsException ex) {
		}
	}

	public static void createIndex() throws Throwable {
		System.out.println("Creating index...");
		dropIndex();
		table.createIndex(columnA);
		table.createIndex(columnsAB);
		table.createIndex(columnC);
	}

	public static void populateTable() throws Throwable {
		System.out.println("Populating...");
		table.setAutoFlushTo(false);
		for (int i = 0; i < numRows; i++) {
			//byte[] row = Bytes.toBytes("row" + prefixZeroes("" + i));
			//byte[] valueA = Bytes.toBytes("value" + i % 10);
			//byte[] valueB = Bytes.toBytes("value" + i % 3);
			//byte[] valueC = Bytes.toBytes("value" + i % 6);
      //byte[] row = Bytes.toBytes(i);
      //byte[] valueA = Bytes.toBytes(i % 10);
      //byte[] valueB = Bytes.toBytes(i % 3);
      //byte[] valueC = Bytes.toBytes(i % 6);
      byte[] row = getBytes(i);
      byte[] valueA = getBytes(i % 10);
      byte[] valueB = getBytes(i % 3);
      byte[] valueC = getBytes(i % 6);
      Put put = new Put(row);
      put.add(family, qualifierA, valueA);
      put.add(family, qualifierB, valueB);
			put.add(family, qualifierC, valueC);
			table.put(put);
		}
		table.flushCommits();
		table.setAutoFlushTo(true);
	}

  public static void scanTable() throws Throwable {
    System.out.println("Scanning...");
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    while (true) {
      Result result = scanner.next();
      if (result == null) {
        break;
      }
      System.out.println(resultToString(result));
    }
  }

	public static void testEqualsQuery(int a) throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + columnA.toString() + " = " + a);
		//byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
		Criterion<?> criterion = new ByteArrayCriterion(columnA, valueA);
		IndexedColumnQuery query = new IndexedColumnQuery(criterion);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testGreaterThanQuery(int a) throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + columnA.toString() + " > " + a);
		//byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
		Criterion<?> criterion =
				new ByteArrayCriterion(columnA, valueA, CompareType.GREATER);
		IndexedColumnQuery query = new IndexedColumnQuery(criterion);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testLessThanOrEqualsQuery(int a) throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + columnA.toString() + " <= " + a);
		//byte[] valueA = Bytes.toBytes(a);
    byte[] valueA = getBytes(a);
		Criterion<?> criterion =
				new ByteArrayCriterion(columnA, valueA, CompareType.LESS_OR_EQUAL);
		IndexedColumnQuery query = new IndexedColumnQuery(criterion);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testRangeQuery(int a, int b) throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
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

	public static void testQuerySelectAB(int a, int b)
	throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
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

	public static void testQuerySelectBC(int b, int c)
	throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
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

	public static void testQuerySelectAC(int a, int c)
			throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
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

	public static void testQueryProjectBC(int a) throws Throwable {
		System.out.println(
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

	public static void testQuerySelectABProjectBC(int a, int b)
	throws Throwable {
		System.out.println(
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

	public static void printResults(List<Result> results) {
		if (results == null) return;
		for (Result result : results) {
			System.out.println(resultToString(result));
		}
		System.out.println();
	}

	public static String resultToString(Result result) {
    String str = "";
    byte[] row = result.getRow();
    byte[] valueA = result.getValue(family, qualifierA);
    byte[] valueB = result.getValue(family, qualifierB);
    byte[] valueC = result.getValue(family, qualifierC);
		//str = str + (row != null ? Bytes.toInt(row) : "null") + ": ";
    //str = str + (valueA != null ? Bytes.toInt(valueA) : "null") + ", ";
    //str = str + (valueB != null ? Bytes.toInt(valueB) : "null") + ", ";
    //str = str + (valueC != null ? Bytes.toInt(valueC) : "null") + ", ";
    str = str + (row != null ? getInt(row) : "null") + ": ";
    str = str + (valueA != null ? getInt(valueA) : "null") + ", ";
    str = str + (valueB != null ? getInt(valueB) : "null") + ", ";
    str = str + (valueC != null ? getInt(valueC) : "null") + ", ";
    return str;
	}

	public static String prefixZeroes(String row) {
		int maxDigits = ("" + numRows).length();
		int prefixDigits = maxDigits - row.length();
		for (int j = 0; j < prefixDigits; j++) row = "0" + row;
		return row;
	}

  public static void testIntComparison() {
    int flagbit32 = (int) Math.pow(2, 31);   // 2^^31
    byte[] zero = getBytes(0);
    byte[] one = getBytes(1);
    byte[] _one = getBytes(-1);
    int intZero = getInt(zero);
    int intOne = getInt(one);
    int int_One = getInt(_one);
    System.out.println("" + intZero + " " + intOne + " " + int_One);
    System.out.println("0 , 0  " + Bytes.compareTo(zero, zero));
    System.out.println("0 , 1  " + Bytes.compareTo(zero, one));
    System.out.println("0 , -1  " + Bytes.compareTo(zero, _one));
    System.out.println("1 , 0  " + Bytes.compareTo(one, zero));
    System.out.println("1 , 1  " + Bytes.compareTo(one, one));
    System.out.println("1 , -1  " + Bytes.compareTo(one, _one));
    System.out.println("-1 , 0  " + Bytes.compareTo(_one, zero));
    System.out.println("-1 , 1  " + Bytes.compareTo(_one, one));
    System.out.println("-1 , -1  " + Bytes.compareTo(_one, _one));
  }

  private static PositionedByteRange intWrapper = new SimplePositionedByteRange(5);
  public static byte[] getBytes(int i) {
    OrderedBytes.encodeInt32(intWrapper.set(5), i, Order.ASCENDING);
    return intWrapper.getBytes();
  }
  public static int getInt(byte[] b) {
    return OrderedBytes.decodeInt32(intWrapper.set(b));
  }
}
