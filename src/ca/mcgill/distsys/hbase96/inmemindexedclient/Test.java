package ca.mcgill.distsys.hbase96.inmemindexedclient;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.Util;
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
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test {

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
			byte[] row = Bytes.toBytes("row" + prefixZeroes("" + i));
			byte[] valueA = Bytes.toBytes("value" + i % 10);
			byte[] valueB = Bytes.toBytes("value" + i % 3);
			byte[] valueC = Bytes.toBytes("value" + i % 6);
			Put put = new Put(row);
			put.add(family, qualifierA, valueA);
			put.add(family, qualifierB, valueB);
			put.add(family, qualifierC, valueC);
			table.put(put);
		}
		table.flushCommits();
		table.setAutoFlushTo(true);
	}

	public static void testEqualsQuery(String a) throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + columnA.toString() + " = " + a);
		byte[] valueA = Bytes.toBytes(a);
		Criterion<?> criterion = new ByteArrayCriterion(columnA, valueA);
		IndexedColumnQuery query = new IndexedColumnQuery(criterion);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testGreaterThanQuery(String a) throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + columnA.toString() + " > " + a);
		byte[] valueA = Bytes.toBytes(a);
		Criterion<?> criterion =
				new ByteArrayCriterion(columnA, valueA, CompareType.GREATER);
		IndexedColumnQuery query = new IndexedColumnQuery(criterion);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testLessThanOrEqualsQuery(String a) throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + columnA.toString() + " <= " + a);
		byte[] valueA = Bytes.toBytes(a);
		Criterion<?> criterion =
				new ByteArrayCriterion(columnA, valueA, CompareType.LESS_OR_EQUAL);
		IndexedColumnQuery query = new IndexedColumnQuery(criterion);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testRangeQuery(String a, String b) throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + a + " <= " + columnA.toString() + " <= " + b);
		byte[] valueA = Bytes.toBytes(a);
		byte[] valueB = Bytes.toBytes(b);
		Criterion<?> criterion = new ByteArrayCriterion(columnA, valueA, valueB);
		IndexedColumnQuery query = new IndexedColumnQuery(criterion);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testQuerySelectAB(String a, String b)
	throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + columnA.toString() + " = " + a +
				" AND " + columnB.toString() + " = " + b);
		byte[] valueA = Bytes.toBytes(a);
		byte[] valueB = Bytes.toBytes(b);
		Criterion<?> criterionA = new ByteArrayCriterion(columnA, valueA);
		Criterion<?> criterionB = new ByteArrayCriterion(columnB, valueB);
		List<Criterion<?>> criteria =
				new ArrayList(Arrays.asList(criterionA, criterionB));
		IndexedColumnQuery query = new IndexedColumnQuery(criteria);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testQuerySelectBC(String b, String c)
	throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + columnB.toString() + " = " + b +
				" AND " + columnC.toString() + " = " + c);
		byte[] valueB = Bytes.toBytes(b);
		byte[] valueC = Bytes.toBytes(c);
		Criterion<?> criterionB = new ByteArrayCriterion(columnB, valueB);
		Criterion<?> criterionC = new ByteArrayCriterion(columnC, valueC);
		List<Criterion<?>> criteria =
				new ArrayList(Arrays.asList(criterionB, criterionC));
		IndexedColumnQuery query = new IndexedColumnQuery(criteria);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testQuerySelectAC(String a, String c)
			throws Throwable {
		System.out.println("SELECT * FROM " + tableName +
				" WHERE " + columnA.toString() + " = " + a +
				" AND " + columnC.toString() + " = " + c);
		byte[] valueA = Bytes.toBytes(a);
		byte[] valueC = Bytes.toBytes(c);
		Criterion<?> criterionA = new ByteArrayCriterion(columnA, valueA);
		Criterion<?> criterionC = new ByteArrayCriterion(columnC, valueC);
		List<Criterion<?>> criteria =
				new ArrayList(Arrays.asList(criterionA, criterionC));
		IndexedColumnQuery query = new IndexedColumnQuery(criteria);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testQueryProjectBC(String a) throws Throwable {
		System.out.println(
				"SELECT id, " + columnB.toString() + ", " + columnC.toString() +
				" FROM " + tableName +
				" WHERE " + columnA.toString() + " = " + a);
		byte[] valueA = Bytes.toBytes(a);
		Criterion<?> criterion = new ByteArrayCriterion(columnA, valueA);
		List<Criterion<?>> criteria = new ArrayList(Arrays.asList(criterion));
		IndexedColumnQuery query = new IndexedColumnQuery(criteria, columnsBC);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void testQuerySelectABProjectBC(String a, String b)
	throws Throwable {
		System.out.println(
				"SELECT id, " + columnB.toString() + ", " + columnC.toString() +
						" WHERE " + columnA.toString() + " = " + a +
						" AND " + columnB.toString() + " = " + b);
		byte[] valueA = Bytes.toBytes(a);
		byte[] valueB = Bytes.toBytes(b);
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
		return
				Bytes.toString(result.getRow()) + ": " +
				Bytes.toString(result.getValue(family, qualifierA)) + ", " +
				Bytes.toString(result.getValue(family, qualifierB)) + ", " +
				Bytes.toString(result.getValue(family, qualifierC));
	}

	public static String prefixZeroes(String row) {
		int maxDigits = ("" + numRows).length();
		int prefixDigits = maxDigits - row.length();
		for (int j = 0; j < prefixDigits; j++) row = "0" + row;
		return row;
	}
}
