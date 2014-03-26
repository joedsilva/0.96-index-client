package ca.mcgill.distsys.hbase96.inmemindexedclient;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.exceptions.IndexNotExistsException;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion.CompareType;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class Test {

  public static final boolean CLEAN = true;

  public static final String tableName = "test";
  public static final byte[] family = Bytes.toBytes("cf");
  public static final byte[] qualifier = Bytes.toBytes("a");

  public static final int numRows = 30;

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
      initialize();
      testEqualsQuery();
      testGreaterThanQuery();
      testLessThanOrEqualsQuery();
      testRangeQuery();
    } catch (Throwable t) {
      t.printStackTrace();
    } try {
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
      table.deleteIndex(family, qualifier);
    } catch (IndexNotExistsException ex) {}
  }
  public static void createIndex() throws Throwable {
    System.out.println("Creating index...");
    dropIndex();
    table.createIndex(family, qualifier);
  }

  public static void populateTable() throws Throwable {
    System.out.println("Populating...");
    table.setAutoFlushTo(false);
    for (int i = 0; i < numRows; i++) {
      byte[] row = Bytes.toBytes("row" + prefixZeroes(""+i));
      byte[] value = Bytes.toBytes("value" + i%10);
      Put put = new Put(row);
      put.add(family, qualifier, value);
      table.put(put);
    }
    table.flushCommits();
    table.setAutoFlushTo(true);
  }

  public static void testEqualsQuery() throws Throwable {
    System.out.println("Equals query [value2]");
    byte[] a = Bytes.toBytes("value2");
    List<Result> results = table.execIndexedQuery(family, qualifier, a);
    printResults(results);
  }

  public static void testGreaterThanQuery() throws Throwable {
    System.out.println("GreaterThan query [value7]");
    byte[] a = Bytes.toBytes("value7");
    List<Result> results = table.execIndexedQuery(family, qualifier, a,
        CompareType.GREATER);
    printResults(results);
  }

  public static void testLessThanOrEqualsQuery() throws Throwable {
    System.out.println("LessThanOrEquals query [value3]");
    byte[] a = Bytes.toBytes("value3");
    List<Result> results = table.execIndexedQuery(family, qualifier, a,
        CompareType.LESS_OR_EQUAL);
    printResults(results);
  }

  public static void testRangeQuery() throws Throwable {
    System.out.println("Range query [value3, value7]");
    byte[] a = Bytes.toBytes("value3");
    byte[] b = Bytes.toBytes("value7");
    List<Result> results = table.execIndexedQuery(family, qualifier, a, b);
    printResults(results);
  }

  public static void printResults(List<Result> results) {
    if (results == null) return;
    for (Result result : results) {
      System.out.println(resultToString(result));
    }
  }

  public static String resultToString(Result result) {
    return
        Bytes.toString(result.getRow()) + ": " +
        Bytes.toString(result.getValue(family, qualifier));
  }
  public static String prefixZeroes(String row) {
    int maxDigits = ("" + numRows).length();
    int prefixDigits = maxDigits - row.length();
    for(int j = 0; j < prefixDigits; j++) row = "0" + row;
    return row;
  }
}
