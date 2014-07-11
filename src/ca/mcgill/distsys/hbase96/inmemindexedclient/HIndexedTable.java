package ca.mcgill.distsys.hbase96.inmemindexedclient;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.IndexedColumn;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.Util;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.exceptions.IndexAlreadyExistsException;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.exceptions.IndexNotExistsException;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.AbstractPluggableIndex;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorInMemService;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

// Modified by Cong
//import org.apache.hadoop.hbase.exceptions.MasterNotRunningException;
//import org.apache.hadoop.hbase.exceptions.ZooKeeperConnectionException;

public class HIndexedTable extends HTable {

	private static final int DELETE_INDEX = 0;
	private static final int CREATE_INDEX = 1;

	private static Log LOG = LogFactory.getLog(HIndexedTable.class);

	public HIndexedTable(byte[] tableName, HConnection connection,
			ExecutorService pool) throws IOException {
		super(tableName, connection, pool);
	}

	public HIndexedTable(Configuration conf, byte[] tableName,
			ExecutorService pool) throws IOException {
		super(conf, tableName, pool);
	}

	public HIndexedTable(Configuration conf, byte[] tableName)
	throws IOException {
		super(conf, tableName);
	}

	public HIndexedTable(Configuration conf, String tableName)
	throws IOException {
		super(conf, tableName);
	}

	/**
	 * Creates an index on this table's family:qualifier column.
	 *
	 * @param family
	 * @param qualifier
	 * @throws Throwable
	 * @throws ServiceException
	 */
	// Modified by Cong
	@Deprecated
	public void createIndex(String family, String qualifier,
			Class<? extends AbstractPluggableIndex> indexClass,
			Object[] arguments)
			throws ServiceException, Throwable {
		createIndex(Bytes.toBytes(family), Bytes.toBytes(qualifier),
				indexClass, arguments);
	}

	/**
	 * Creates an index on this table's family:qualifier column.
	 *
	 * @param family
	 * @param qualifier
	 * @throws Throwable
	 * @throws ServiceException
	 */
	// Modified by Cong
	@Deprecated
	public void createIndex(byte[] family, byte[] qualifier,
			Class<? extends AbstractPluggableIndex> indexClass,
			Object[] arguments)
			throws ServiceException, Throwable {
		Column column = new Column(family, qualifier);
		createIndex(column, indexClass, arguments);
	}

	public void createIndex(Column column,
			Class<? extends AbstractPluggableIndex> indexClass,
			Object[] arguments)
	throws ServiceException, Throwable {
		createIndex(Arrays.asList(column), indexClass, arguments);
	}

	/**
	 * Creates a multi-column index on this table.
	 *
	 * @param columns    list of columns to create index on
	 * @param indexClass type of index (hash, hybrid, etc.)
	 * @throws Throwable
	 * @throws ServiceException
	 */
	public void createIndex(List<Column> columns,
			Class<? extends AbstractPluggableIndex> indexClass,
			Object[] arguments)
			throws ServiceException, Throwable {

		// Sort the list according to the concatenation of family and qualifier
		//Collections.sort(colList);

		// Yousuf: temp fix
		String indexClassString = indexClass.toString().split(" ")[1];
		//

		CreateIndexCallable callable = new CreateIndexCallable(columns,
				indexClassString, arguments);
		Map<byte[], Boolean> results = null;

		checkSecondaryIndexMasterTable();

		updateMasterIndexTable(columns, indexClassString, arguments, CREATE_INDEX);

		results = this.coprocessorService(IndexCoprocessorInMemService.class,
				HConstants.EMPTY_START_ROW, HConstants.LAST_ROW, callable);

		if (results != null) {
			for (byte[] regionName : results.keySet()) {
				if (!results.get(regionName)) {
					LOG.error("Region [" + Bytes.toString(regionName)
							+ "] failed to create the requested index.");
				}
			}
		}
	}

	/**
	 * Deletes an index on this table's family:qualifier column.
	 *
	 * @param family
	 * @param qualifier
	 * @throws Throwable
	 * @throws ServiceException
	 */
	@Deprecated
	public void deleteIndex(byte[] family, byte[] qualifier)
			throws ServiceException, Throwable {
		deleteIndex(new Column(family, qualifier));
	}

	/**
	 * Deletes an index on this table's column.
	 *
	 * @param column
	 * @throws Throwable
	 * @throws ServiceException
	 */
	public void deleteIndex(Column column)
			throws ServiceException, Throwable {
		deleteIndex(Arrays.asList(column));
	}

	/**
	 * Deletes an index on these columns.
	 *
	 * @param columns
	 * @throws Throwable
	 * @throws ServiceException
	 */
	public void deleteIndex(List<Column> columns)
			throws ServiceException, Throwable {

		DeleteIndexCallable callable = new DeleteIndexCallable(columns);
		Map<byte[], Boolean> results = null;

		checkSecondaryIndexMasterTable();

		updateMasterIndexTable(columns, null, null, DELETE_INDEX);

		results = this.coprocessorService(IndexCoprocessorInMemService.class,
				HConstants.EMPTY_START_ROW, HConstants.LAST_ROW, callable);

		if (results != null) {
			for (byte[] regionName : results.keySet()) {
				if (!results.get(regionName)) {
					LOG.error("Region [" + Bytes.toString(regionName)
							+ "] failed to delete the requested index.");
				}
			}
		}
	}

	public List<Result> execIndexedQuery(IndexedColumnQuery query)
	throws ServiceException, Throwable {
		IndexedQueryCallable callable = new IndexedQueryCallable(query);

		Map<byte[], List<Result>> resultMap = null;
		List<Result> result = new ArrayList<Result>();

		resultMap = this.coprocessorService(IndexCoprocessorInMemService.class,
				HConstants.EMPTY_START_ROW, HConstants.LAST_ROW, callable);

		if (resultMap != null) {
			for (List<Result> regionResult : resultMap.values()) {
				result.addAll(regionResult);
			}
			// Collections.sort(result, new ResultComparator());

		}

		return result;
	}

	private void updateMasterIndexTable(List<Column> columns, String indexClass,
			Object[] arguments, int operation)
	throws IOException {

		HTable masterIdxTable = null;

		byte[] indexName = Bytes.toBytes(Util.concatColumnsToString(columns));

		try {
			masterIdxTable = new HTable(getConfiguration(),
					SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME);

			if (operation == CREATE_INDEX) {
				Get idxGet = new Get(getTableName());
				idxGet.addColumn(Bytes.toBytes(
						SecondaryIndexConstants.MASTER_INDEX_TABLE_IDXCOLS_CF_NAME),
						indexName);
				Result rs = masterIdxTable.get(idxGet);

				if (!rs.isEmpty()) {
					String message = "Index already exists for "
							+ Bytes.toString(indexName)
							+ " of table " + Bytes.toString(getTableName());
					LOG.warn(message);
					throw new IndexAlreadyExistsException(message);
				}

				Put idxPut = new Put(getTableName());
				IndexedColumn ic = new IndexedColumn(columns);
				ic.setIndexType(indexClass);
				ic.setArguments(arguments);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);
				oos.writeObject(ic);
				idxPut.add(Bytes.toBytes(
						SecondaryIndexConstants.MASTER_INDEX_TABLE_IDXCOLS_CF_NAME),
						indexName, baos.toByteArray());
				oos.close();
				masterIdxTable.put(idxPut);
			} else if (operation == DELETE_INDEX) {
				// Modified by Cong
				Get idxGet = new Get(getTableName());
				idxGet.addColumn(Bytes.toBytes(
						SecondaryIndexConstants.MASTER_INDEX_TABLE_IDXCOLS_CF_NAME),
						indexName);
				Result rs = masterIdxTable.get(idxGet);

				if (rs.isEmpty()) {
					String message = "Index does't exist for "
							+ Bytes.toString(indexName)
							+ " of table " + Bytes.toString(getTableName());
					LOG.warn(message);
					throw new IndexNotExistsException(message);
				}

				Delete idxDelete = new Delete(getTableName());
				idxDelete.deleteColumn(Bytes.toBytes(
						SecondaryIndexConstants.MASTER_INDEX_TABLE_IDXCOLS_CF_NAME),
						indexName);
				masterIdxTable.delete(idxDelete);

			} else {
				throw new UnsupportedOperationException(
						"Unknown index operation type.");
			}

		} finally {
			if (masterIdxTable != null) {
				masterIdxTable.close();
			}
		}

	}

	// private void checkSecondaryIndexMasterTable() throws
	// MasterNotRunningException, ZooKeeperConnectionException, IOException {

	private void checkSecondaryIndexMasterTable() throws IOException {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(getConfiguration());
			if (!admin.tableExists(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
				HTableDescriptor desc = new HTableDescriptor(
						SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME);
				desc.addFamily(new HColumnDescriptor(
						SecondaryIndexConstants.MASTER_INDEX_TABLE_IDXCOLS_CF_NAME));
				admin.createTable(desc);
			}
		} finally {
			if (admin != null) {
				admin.close();
			}
		}
	}

	// Yousuf
	public void createHashTableIndex(Column column)
			throws Throwable {
		Class indexClass = Class.forName(SecondaryIndexConstants.HASHTABLE_INDEX);
		int maxTreeSize = getConfiguration().getInt(
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
		Object[] arguments = {maxTreeSize, Arrays.asList(column)};
		createIndex(column, indexClass, arguments);
	}

	public void createHybridIndex(Column column)
	throws Throwable {
		Class indexClass = Class.forName(SecondaryIndexConstants.HASHTABLE_INDEX);
		Object[] arguments = {column.getFamily(), column.getQualifier()};
		createIndex(column, indexClass, arguments);
	}

	public void createIndex(Column column)
	throws Throwable {
		Class indexClass = Class.forName(SecondaryIndexConstants.DEFAULT_INDEX);
		Object[] arguments = {column.getFamily(), column.getQualifier()};
		createIndex(column, indexClass, arguments);
	}

	// Multi-column indexing only works with HashTable index
	public void createIndex(List<Column> columns)
			throws Throwable {
		Class indexClass = Class.forName(SecondaryIndexConstants.HASHTABLE_INDEX);
		int maxTreeSize = getConfiguration().getInt(
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);
		Object[] arguments = {maxTreeSize, columns};
		createIndex(columns, indexClass, arguments);
	}
	//
}
