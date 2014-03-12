package ca.mcgill.distsys.hbase96.inmemindexedclient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
// Modified by Cong
//import org.apache.hadoop.hbase.exceptions.MasterNotRunningException;
//import org.apache.hadoop.hbase.exceptions.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.IndexedColumn;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.SecondaryIndexConstants;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.Util;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.exceptions.IndexAlreadyExistsException;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorInMemService;

import com.google.protobuf.ServiceException;

public class HIndexedTable extends HTable {

    private static final int DELETE_INDEX = 0;
    private static final int CREATE_INDEX = 1;
    
    private static Log LOG = LogFactory.getLog(HIndexedTable.class);

    public HIndexedTable(byte[] tableName, HConnection connection, ExecutorService pool) throws IOException {
        super(tableName, connection, pool);
    }

    public HIndexedTable(Configuration conf, byte[] tableName, ExecutorService pool) throws IOException {
        super(conf, tableName, pool);
    }

    public HIndexedTable(Configuration conf, byte[] tableName) throws IOException {
        super(conf, tableName);
    }

    public HIndexedTable(Configuration conf, String tableName) throws IOException {
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
    public void createIndex(String family, String qualifier, String indexType, Object[] arguments) throws ServiceException, Throwable {
        createIndex(Bytes.toBytes(family), Bytes.toBytes(qualifier), indexType, arguments);
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
    public void createIndex(byte[] family, byte[] qualifier, String indexType, Object[] arguments) throws ServiceException, Throwable {

        CreateIndexCallable callable = new CreateIndexCallable(family, qualifier, indexType, arguments);
        Map<byte[], Boolean> results = null;

        checkSecondaryIndexMasterTable();

        updateMasterIndexTable(family, qualifier, CREATE_INDEX);

        results = this.coprocessorService(IndexCoprocessorInMemService.class, HConstants.EMPTY_START_ROW, HConstants.LAST_ROW, callable);

        if (results != null) {
            for (byte[] regionName : results.keySet()) {
                if (!results.get(regionName)) {
                    LOG.error("Region [" + new String(regionName) + "] failed to create the requested index.");
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
    public void deleteIndex(byte[] family, byte[] qualifier) throws ServiceException, Throwable {

        DeleteIndexCallable callable = new DeleteIndexCallable(family, qualifier);
        Map<byte[], Boolean> results = null;

        checkSecondaryIndexMasterTable();

        updateMasterIndexTable(family, qualifier, DELETE_INDEX);
        

        results = this.coprocessorService(IndexCoprocessorInMemService.class, HConstants.EMPTY_START_ROW, HConstants.LAST_ROW, callable);

        
        if (results != null) {
            for (byte[] regionName : results.keySet()) {
                if (!results.get(regionName)) {
                    LOG.error("Region [" + new String(regionName) + "] failed to delete the requested index.");
                }
            }
        }
    }
    
    public List<Result> execIndexedQuery(IndexedColumnQuery query) throws ServiceException, Throwable {
        IndexedQueryCallable callable = new IndexedQueryCallable(query);
       
        Map<byte[], List<Result>> resultMap = null;
        List<Result> result = new ArrayList<Result>();
        
        resultMap = this.coprocessorService(IndexCoprocessorInMemService.class, HConstants.EMPTY_START_ROW, HConstants.LAST_ROW, callable);
        
        if (resultMap != null) {
            for(List<Result> regionResult: resultMap.values()) {
                result.addAll(regionResult);
            }
//            Collections.sort(result, new ResultComparator());

        }
        
        return result;
    }
    
    private void updateMasterIndexTable(byte[] family, byte[] qualifier, int operation) throws IOException {
        HTable masterIdxTable = null;

        try {
            masterIdxTable = new HTable(getConfiguration(), SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME);
            
            if(operation == CREATE_INDEX) {
            Get idxGet = new Get(getTableName());
            idxGet.addColumn(Bytes.toBytes(SecondaryIndexConstants.MASTER_INDEX_TABLE_IDXCOLS_CF_NAME), Util.concatByteArray(family, qualifier));
            Result rs = masterIdxTable.get(idxGet);
            
            if (!rs.isEmpty()) {
                LOG.warn("Index already exists for " + new String(family) + ":" + new String(qualifier) + " of table " + new String(getTableName()));
                throw new IndexAlreadyExistsException("Index already exists for " + new String(family) + ":" + new String(qualifier) + " of table "
                        + new String(getTableName()));
            }

            Put idxPut = new Put(getTableName());
            IndexedColumn ic = new IndexedColumn(family, qualifier);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(ic);
            idxPut.add(Bytes.toBytes(SecondaryIndexConstants.MASTER_INDEX_TABLE_IDXCOLS_CF_NAME), Util.concatByteArray(family, qualifier),
                    baos.toByteArray());
            oos.close();
            masterIdxTable.put(idxPut);
            } else if (operation == DELETE_INDEX){
            	// Modified by Cong
                
            } else {
                throw new UnsupportedOperationException("Unknown index operation type.");
            }

        } finally {
            if (masterIdxTable != null) {
                masterIdxTable.close();
            }
        }

    }

//    private void checkSecondaryIndexMasterTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        
    private void checkSecondaryIndexMasterTable() throws  IOException {
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(getConfiguration());
            if (!admin.tableExists(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME)) {
                HTableDescriptor desc = new HTableDescriptor(SecondaryIndexConstants.MASTER_INDEX_TABLE_NAME);
                desc.addFamily(new HColumnDescriptor(SecondaryIndexConstants.MASTER_INDEX_TABLE_IDXCOLS_CF_NAME));
                admin.createTable(desc);
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }
}
