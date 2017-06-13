package ca.mcgill.distsys.hbase96.indexclient;

import ca.mcgill.distsys.hbase96.indexcommons.proto.Column;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Random;

/**
 * Created by joseph on 27/05/16.
 */
public abstract class TCBase extends Thread
{


    /** Static fields **/
    protected static Log LOG = LogFactory.getLog(TCBase.class);
    protected static Properties properties = System.getProperties();

    protected static final int NUMRECORDS = 10000;
    protected static int numRecords;
    protected static final int NUMREADS = 10;
    protected static int numReads;
    protected static final int SELECTIVITY = 1;
    protected static int selectivity;
    protected static final int NUMTHREADS = 1;
    protected static int numThreads;

    protected static final String NAMESPACE = "test";
    protected static String namespace;
    protected static final String TABLENAME = "Test_01";
    protected static String tablename;
    protected static final int NUMREGIONS = 1;
    protected static int numRegions;
    protected static int recordsPerRegion;
    protected static final int NUMFIELDS = 10;
    protected static int numFields;
    protected static String COLUMNFAMILY = "cf";
    protected static byte[] columnFamily;
    protected static byte[][] qualifiers;
    protected static Column[] columns;
    protected static boolean createDDLOnly;

    protected static enum TestType { READ, RANGE, INSERT, UPDATE, DELETE };
    protected static TestType testType;

    protected static final int READFIELD = 1;
    protected static int readField;
    protected static final int PROJECTFIELD = 2;
    protected static int projectField;
    protected static final int READNUMRECORDS = 1000;
    protected static int readNumRecords;
    protected static final int READEXTRASKIPRECORDS = 100;
    protected static int readExtraSkipRecords;
    protected static int resultBufferSize;
    protected static boolean printResult;
    protected static final int UPDATEFIELD = 1;
    protected static int updateField;
    protected static final int NUMUPDATES = 1000;
    protected static int numUpdates;
    protected static final int NUMEXTRAUPDATES=100;
    protected static int numExtraUpdates;
    protected static final int NUMDELETES = 1000;
    protected static int numDeletes;
    protected static final int NUMEXTRADELETES=100;
    protected static int numExtraDeletes;
    //protected final static int RANGEFIELD = READFIELD;
    protected static int rangeField;
    protected static final int NUMRANGES = 100;
    protected static int numRanges;
    protected static int maxRange;

    protected static boolean batchMode;

    protected static int timeLogRecords;

    protected static final boolean ROWKEYSEQUENTIAL = true;
    protected static boolean rowkeySequential;
    protected static final boolean ROWKEYSALTERNATEREGIONS = false;
    protected static boolean rowkeysAlternateRegions;
    protected static final float ROWKEYDISTMULTIPLIER = 1;
    protected static float rowkeyDistMultiplier;

    protected static Configuration conf;
    protected static HBaseAdmin admin;

    protected static Random siRandom;
    protected static Random keyRandom;
    protected static int maxSIVal;
    protected static int numSIRegions;


    /** static fields for Index based test cases  */
    protected static boolean createIndex;
    //protected static final int INDEXFIELD = READFIELD;
    protected static int indexField;
    protected static HIndexedTable idxTable;

    /** non-static fields **/
    protected int threadId;

    public TCBase(int threadid)
    {
        this.threadId = threadid;
    }

    public void run()
    {
        LOG.info("Thread " + threadId + " starting ... ");

        switch (testType)
        {
            case INSERT: if(batchMode) doBatchInserts(); else doInserts(); break;
            case READ: doReads(); break;
            case RANGE: doRangeReads(); break;
            case UPDATE: if(batchMode) doBatchUpdates(); else doUpdates(); break;
            case DELETE: if(batchMode) doBatchDeletes(); else doDeletes(); break;
            default: LOG.error("Unable to detect test type");
        }

        LOG.info("Thread " + threadId + " terminating ... ");
    }

    protected abstract void doInserts();
    protected abstract void doReads();
    protected abstract void doRangeReads();
    protected abstract void doUpdates();
    protected abstract void doDeletes();
    protected abstract void doBatchInserts();
    protected abstract void doBatchUpdates();
    protected abstract void doBatchDeletes();


    protected static void printResults(List<Result> results)
    {
        for (int i=0; i<results.size(); i++)
        {
            String record = "{ ";
            NavigableMap<byte[], NavigableMap<byte[],byte[]>> map = results.get(i).getNoVersionMap();
            for (byte[] family : map.keySet())
                for(byte[] column : map.get(family).navigableKeySet())
                    record += " { " + new String(family) + ":" + new String(column) + " = " + new String(results.get(i).getValue(family,column)) + " } ";
            record += " }'";

            System.out.println(record);
        }
    }

    protected static void printResults(Result[] results)
    {
        for (int i=0; i<results.length; i++)
        {
            String record = "{ ";
            NavigableMap<byte[], NavigableMap<byte[],byte[]>> map = results[i].getNoVersionMap();
            for (byte[] family : map.keySet())
                for(byte[] column : map.get(family).navigableKeySet())
                    record += " { " + new String(family) + ":" + new String(column) + " = " + new String(results[i].getValue(family,column)) + " } ";
            record += " }'";

            System.out.println(record);
        }
    }

    protected static void setProperties(String args[])
    {

        LOG.info("Listing properties ...");
        properties.list(System.out);
        numRecords = Integer.parseInt(properties.getProperty("NUMRECORDS", ""+NUMRECORDS));
        numReads = Integer.parseInt(properties.getProperty("NUMREADS", ""+NUMREADS));
        selectivity = Integer.parseInt(properties.getProperty("SELECTIVITY", ""+SELECTIVITY));
        numThreads = Integer.parseInt(properties.getProperty("NUMTHREADS", ""+NUMTHREADS));

        namespace = properties.getProperty("NAMESPACE", NAMESPACE);
        tablename = properties.getProperty("TABLENAME", TABLENAME);
        numRegions = Integer.parseInt(properties.getProperty("NUMREGIONS", ""+NUMREGIONS));
        recordsPerRegion = numRecords/numRegions;
        numSIRegions = Integer.parseInt(properties.getProperty("NUMSIREGIONS", ""+numRegions));
        numFields = Integer.parseInt(properties.getProperty("NUMFIELDS", ""+NUMFIELDS));
        columnFamily = properties.getProperty("COLUMNFAMILY", COLUMNFAMILY).getBytes();
        createDDLOnly = Boolean.parseBoolean(properties.getProperty("CREATEDDLONLY", "false"));

        readField = Integer.parseInt(properties.getProperty("READFIELD", ""+READFIELD));
        readNumRecords = Integer.parseInt(properties.getProperty("READNUMRECORDS", ""+READNUMRECORDS));
        readExtraSkipRecords = Integer.parseInt(properties.getProperty("READEXTRASKIPRECORDS", ""+READEXTRASKIPRECORDS));
        projectField = Integer.parseInt(properties.getProperty("PROJECTFIELD", ""+PROJECTFIELD));
        resultBufferSize = Integer.parseInt(properties.getProperty("RESULTBUFFERSIZE", ""+SELECTIVITY));
        printResult = Boolean.parseBoolean(properties.getProperty("PRINTRESULT", "false"));
        numUpdates = Integer.parseInt(properties.getProperty("NUMUPDATES", ""+NUMUPDATES));
        numExtraUpdates = Integer.parseInt(properties.getProperty("NUMEXTRAUPDATES", ""+NUMEXTRAUPDATES));
        updateField = Integer.parseInt(properties.getProperty("UPDATEFIELD", ""+UPDATEFIELD));
        numDeletes = Integer.parseInt(properties.getProperty("NUMDELETES", ""+NUMDELETES));
        numExtraDeletes = Integer.parseInt(properties.getProperty("NUMEXTRADELETES", ""+NUMEXTRADELETES));
        rangeField =  Integer.parseInt(properties.getProperty("RANGEFIELD", ""+readField));
        numRanges =  Integer.parseInt(properties.getProperty("NUMRANGES", ""+NUMRANGES));

        batchMode =  Boolean.parseBoolean(properties.getProperty("BATCHMODE", "false"));

        timeLogRecords = Integer.parseInt(properties.getProperty("TIMELOGRECORDS", ""+numRecords));

        rowkeySequential = Boolean.parseBoolean(properties.getProperty("ROWKEYSEQUENTIAL", ""+ROWKEYSEQUENTIAL));
        rowkeysAlternateRegions =  Boolean.parseBoolean(properties.getProperty("ROWKEYSALTERNATEREGIONS", ""+ROWKEYSALTERNATEREGIONS));
        rowkeyDistMultiplier =  Float.parseFloat(properties.getProperty("ROWKEYDISTMULTIPLIER", ""+ROWKEYDISTMULTIPLIER));
        maxSIVal = (int)(rowkeyDistMultiplier *numRecords) / selectivity;

        maxRange =  Integer.parseInt(properties.getProperty("MAXRANGE", ""+maxSIVal));

        switch(properties.getProperty("TESTTYPE"))
        {
            case "READ": testType = TestType.READ; break;
            case "RANGE": testType = TestType.RANGE; break;
            case "UPDATE": testType = TestType.UPDATE; break;
            case "DELETE": testType = TestType.DELETE; break;
            case "INSERT":default: testType = TestType.INSERT; break;
        }
        LOG.info("testType = " + testType);

        createIndex = Boolean.parseBoolean(properties.getProperty("CREATEINDEX", "false"));
        indexField  = Integer.parseInt(properties.getProperty("INDEXFIELD", ""+readField));

    }

    protected static void setColumns()
    {
        qualifiers = new byte[numFields][];
        columns = new Column[numFields];
        for(int i=0; i<numFields; i++)
        {
            qualifiers[i] = String.format("c_%02d", i+1).getBytes();
            columns[i] = new Column(columnFamily,qualifiers[i]);
        }
    }

    protected static void createTable() throws Exception
    {
        createTable(0);
    }
    protected static void createTable(int numRegions) throws Exception
    {
        //HTableDescriptor td = new HTableDescriptor(TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
        HTableDescriptor td = new HTableDescriptor(TableName.valueOf(tablename.getBytes()));
        td.addFamily(new HColumnDescriptor(columnFamily));

        if(numRegions <= 1)
            admin.createTable(td);
        else
        {
            int recordsPerRegion = (rowkeySequential ? numRecords : (int)(numRecords*rowkeyDistMultiplier)) / numRegions;
            byte[] startKey = String.format("row_%012d", recordsPerRegion).getBytes();

            if(numRegions==2)
            {
               byte[][] splitKeys = new byte[1][];
               splitKeys[0] = startKey;
               admin.createTable(td, splitKeys);
            }
            else
            {
               byte[] endKey = String.format("row_%012d", (rowkeySequential ? numRecords : (int) (numRecords * rowkeyDistMultiplier)) - recordsPerRegion).getBytes();
               admin.createTable(td, startKey, endKey, numRegions);
            }
        }
    }

    public static void waitForThreads(Thread[] threads)
    {
        for (int t = 0; t < threads.length; t++)
        {
            if (threads[t] != null)
            {
                try { threads[t].join(); } catch (InterruptedException ex) { }
                threads[t] = null;
            }
        }
    }

    protected static void initialize(String args[]) throws Exception
    {
        setProperties(args);
        conf = HBaseConfiguration.create();
        admin = new HBaseAdmin(conf);

        siRandom = new Random();
        keyRandom = new Random();
        //maxSIVal = (int)(rowkeyDistMultiplier *numRecords) / selectivity;

        //boolean tableExists = admin.tableExists(TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
        boolean tableExists = admin.tableExists(TableName.valueOf(tablename.getBytes()));
        LOG.info("Table exists = " + tableExists);

        if(testType == TestType.INSERT)
        {
            if (tableExists)
            {
                //admin.disableTable(TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
                //admin.deleteTable(TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
                admin.disableTable(TableName.valueOf(tablename.getBytes()));
                admin.deleteTable(TableName.valueOf(tablename.getBytes()));
            }

            setColumns();
            createTable(numRegions);
        }
        else if(testType == TestType.READ || testType == TestType.RANGE || testType == TestType.UPDATE)
        { setColumns(); }
    }

    protected static void cleanup() throws Exception
    {
        admin.close();
    }

}
