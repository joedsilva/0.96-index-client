package ca.mcgill.distsys.hbase96.inmemindexedclient;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

	/**
	 * @param args
	 * @throws Throwable 
	 * @throws ServiceException 
	 */
	public static void main(String[] args) throws ServiceException, Throwable {
		// TODO Auto-generated method stub

		System.out.println("Test coprocessor....");

		Configuration config = HBaseConfiguration.create();
		try {
			HIndexedTable table = new HIndexedTable(config, "coprocessor");
			String indexType = "ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.hybridBased.HybridIndex";
			//String indexType = "ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.hashtableBased.RegionColumnIndex";

			Object[] arguments = new Object[2];
			int maxTreeSize = config.getInt(
					SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
					SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
			//arguments[0] = maxTreeSize;
			arguments[0] = Bytes.toBytes("cf");
			arguments[1] = Bytes.toBytes("a");
			try {
				table.createIndex("cf", "a", indexType, arguments);
			} catch (Throwable e) {
				System.out.println("Hello World");
				table.deleteIndex(Bytes.toBytes("cf"), Bytes.toBytes("a"));
			} finally {
				// table.close();
			}

			Put put = new Put(Bytes.toBytes("row2"));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("a"),
					Bytes.toBytes("value2"));
			table.put(put);

			IndexedColumnQuery query = new IndexedColumnQuery();
			query.setMustPassAllCriteria(true);
			ByteArrayCriterion criterion = new ByteArrayCriterion(
					Bytes.toBytes("value2"));
			criterion.setCompareColumn(new Column(Bytes.toBytes("cf"))
					.setQualifier(Bytes.toBytes("a")));
			criterion.setComparisonType(CompareType.EQUAL);
			query.addCriterion(criterion);
			List<Result> results = table.execIndexedQuery(query);
			
			System.out.println("result size is: " + results.size());
			System.out.println("The row key is: " + Bytes.toString(results.get(0).getRow()));
			
			// Range query
			IndexedColumnQuery rangeQuery = new IndexedColumnQuery();
			rangeQuery.setMustPassAllCriteria(true);
			ByteArrayCriterion rangeCriterion = new ByteArrayCriterion(
					Bytes.toBytes("value1"));
			rangeCriterion.setCompareColumn(new Column(Bytes.toBytes("cf"))
					.setQualifier(Bytes.toBytes("a")));
			rangeCriterion.setComparisonType(CompareType.RANGE);
			rangeCriterion.setRange(Bytes.toBytes("value3"), Bytes.toBytes("value8"));
			rangeQuery.addCriterion(rangeCriterion);
			List<Result> rangeResults = table.execIndexedQuery(rangeQuery);
			
			System.out.println("range result size is: " + rangeResults.size());
			for (Result result: rangeResults) {
				System.out.println("The row key is: " + Bytes.toString(result.getRow()));
			}
			
			System.out.println("Done....");
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}