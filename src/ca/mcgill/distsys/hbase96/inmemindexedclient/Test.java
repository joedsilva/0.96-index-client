package ca.mcgill.distsys.hbase96.inmemindexedclient;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.ByteArrayCriterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion.CompareType;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.IndexedColumnQuery;

import com.google.protobuf.ServiceException;

public class Test {

	public static final String tableName = "coprocessors";
	public static final byte[] family = Bytes.toBytes("cf");
	public static final byte[] qualifier = Bytes.toBytes("a");

	public static final int numRows = 30;

	public static Configuration conf;
	public static HBaseAdmin admin;
	public static HIndexedTable table;

	public static void initialize() throws Throwable {
		conf = HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);
		createTable();
		createIndex();
		populateTable();
	}

	/**
	 * @param args
	 * @throws Throwable
	 * @throws ServiceException
	 */
	public static void main(String[] args) throws IOException {
		// try {
		// initialize();
		// testEqualsQuery();
		// testRangeQuery();
		// } catch (Throwable t) {
		// t.printStackTrace();
		// } try {
		// table.close();
		// } catch (Throwable t) {
		// t.printStackTrace();
		// }

		System.out.println("Hello World~~~");
		conf = HBaseConfiguration.create();
		table = new HIndexedTable(conf, tableName);

		String namespace = "ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex";
		String indexType = namespace + ".hybridBased2.HybridIndex";
		// String indexType = namespace + ".hashtableBased.RegionColumnIndex";
		int maxTreeSize = conf.getInt(
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
		// arguments[0] = maxTreeSize;
		Object[] arguments = { family, qualifier };
		try {
			
			
			// create index
			System.out.println("creating index for table: "
					+ new String(table.getTableName()) + ":"
					+ new String(family) + ":" + new String(qualifier)
					+ "......");
//			table.createIndex(family, qualifier, indexType, arguments);
			
			// test range query
			System.out.println("Range query [value1, value8]");
			byte[] a = Bytes.toBytes("value1");
			byte[] b = Bytes.toBytes("value8");
			IndexedColumnQuery query = buildIndexedQuery(family, qualifier, a, b);
			List<Result> results = table.execIndexedQuery(query);
			printResults(results);
			
			query = buildIndexedQuery(family, qualifier, Bytes.toBytes("value1"));
			results = table.execIndexedQuery(query);
			System.out.println("point query....");
			printResults(results);
			
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("deleting index for table: "
					+ new String(table.getTableName()) + ":"
					+ new String(family) + ":" + new String(qualifier)
					+ "......");
			try {
				table.deleteIndex(family, qualifier);
			} catch (ServiceException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (Throwable e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
		} finally {
			table.close();
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
		table = new HIndexedTable(conf, tableName);
	}

	public static void dropIndex() throws Throwable {
		table.deleteIndex(family, qualifier);
		String sysIndexTable = "__sys__indextable";
		if (admin.tableExists(sysIndexTable)) {
			admin.disableTable(sysIndexTable);
			admin.deleteTable(sysIndexTable);
		}
	}

	public static void createIndex() throws Throwable {
		System.out.println("Creating index...");
		dropIndex();
		String namespace = "ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex";
		String indexType = namespace + ".hybridBased.HybridIndex";
		// String indexType = namespace + ".hashtableBased.RegionColumnIndex";
		int maxTreeSize = conf.getInt(
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
		// arguments[0] = maxTreeSize;
		Object[] arguments = { family, qualifier };
		table.createIndex(family, qualifier, indexType, arguments);
	}

	public static void populateTable() throws Throwable {
		System.out.println("Populating...");
		table.setAutoFlushTo(false);
		for (int i = 0; i < numRows; i++) {
			byte[] row = Bytes.toBytes("row" + prefixZeroes("" + i));
			byte[] value = Bytes.toBytes("value" + i % 10);
			Put put = new Put(row);
			put.add(family, qualifier, value);
			table.put(put);
		}
		table.flushCommits();
		table.setAutoFlushTo(true);
	}

	public static IndexedColumnQuery buildIndexedQuery(byte[] family,
			byte[] qualifier, byte[] value) {
		IndexedColumnQuery query = new IndexedColumnQuery();
		query.setMustPassAllCriteria(true);
		ByteArrayCriterion criterion = new ByteArrayCriterion(value);
		criterion.setCompareColumn(new Column(family).setQualifier(qualifier));
		criterion.setComparisonType(CompareType.EQUAL);
		query.addCriterion(criterion);
		return query;
	}

	public static void testEqualsQuery() throws Throwable {
		System.out.println("Equals query [value2]");
		byte[] a = Bytes.toBytes("value2");
		IndexedColumnQuery query = buildIndexedQuery(family, qualifier, a);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static IndexedColumnQuery buildIndexedQuery(byte[] family,
			byte[] qualifier, byte[] valueA, byte[] valueB) {
		IndexedColumnQuery query = new IndexedColumnQuery();
		query.setMustPassAllCriteria(true);
		ByteArrayCriterion criterion = new ByteArrayCriterion(valueA);
		criterion.setCompareColumn(new Column(family).setQualifier(qualifier));
		criterion.setComparisonType(CompareType.RANGE);
		criterion.setRange(valueA, valueB);
		query.addCriterion(criterion);
		return query;
	}

	public static void testRangeQuery() throws Throwable {
		System.out.println("Range query [value3, value8]");
		byte[] a = Bytes.toBytes("value3");
		byte[] b = Bytes.toBytes("value8");
		IndexedColumnQuery query = buildIndexedQuery(family, qualifier, a, b);
		List<Result> results = table.execIndexedQuery(query);
		printResults(results);
	}

	public static void printResults(List<Result> results) {
		if (results == null)
			return;
		if(results.size() == 0){
			System.out.println("result size is 0....");
			return;
		}
		for (Result result : results) {
			System.out.println(resultToString(result));
		}
	}

	public static String resultToString(Result result) {
		return Bytes.toString(result.getRow()) + ": "
				+ Bytes.toString(result.getValue(family, qualifier));
	}

	public static String prefixZeroes(String row) {
		int maxDigits = ("" + numRows).length();
		int prefixDigits = maxDigits - row.length();
		for (int j = 0; j < prefixDigits; j++)
			row = "0" + row;
		return row;
	}

}
