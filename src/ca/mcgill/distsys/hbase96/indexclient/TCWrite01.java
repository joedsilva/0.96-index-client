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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by joseph on 23/05/16.
 */
public class TCWrite01 extends Thread
{

    /** Static fields **/
    private static Log LOG = LogFactory.getLog(TCWrite01.class);
    private static Properties properties = System.getProperties();

    private static final int NUMRECORDS = 100;
    private static int numRecords;
    private static final int NUMREADS = 10;
    private static int numReads;
    private static final int SELECTIVITY = 1;
    private static int selectivity;
    private static final int NUMTHREADS = 1;
    private static int numThreads;

    private static final String NAMESPACE = "test";
    private static String namespace;
    private static final String TABLENAME = "Test_01";
    private static String tablename;
    private static final int NUMFIELDS = 10;
    private static int numFields;
    private static String COLUMNFAMILY = "cf";
    private static byte[] columnFamily;
    private static byte[][] qualifiers;
    private static Column[] columns;

    private static enum TestType { READ, INSERT, UPDATE, DELETE };
    private static TestType testType;

    private static final int READFIELD = 1;
    private static int readField;
    private static final int PROJECTFIELD = 2;
    private static int projectField;
    private static final int READNUMRECORDS = 1000;
    private static int readNumRecords;
    private static boolean printResult;

    private static Configuration conf;
    private static HBaseAdmin admin;

    /** non-static fields **/
    private int threadId;

    public void run()
    {
        LOG.info("Thread " + threadId + " starting ... ");

        switch (testType)
        {
            case INSERT: doInserts(); break;
            case READ: doReads(); break;
            default: LOG.info("Unable to detect test type");
        }

        LOG.info("Thread " + threadId + " terminating ... ");
    }

    private void doInserts()
    {

        LOG.info("Thread " + threadId + " starting inserts");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);
        int mykeystart = threadId*recordsPerThread;
        int mykeyend   = (threadId+1)*recordsPerThread - 1;

        int mysecstart = mykeystart;
        int mysecend   = mykeystart + (recordsPerThread + selectivity - 1)/selectivity - 1;

        long totalDuration = 0;


        try
        {
          HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
          //htable.setAutoFlushTo(true);
          long startTime = 0;
          for (int i=0,key=mykeystart; i<selectivity; i++)
            for(int sec=mysecstart; sec<=mysecend; sec++, key++)
            {
              //LOG.info("Thread = " + threadId + " key = " + key + " sec = " + sec);

                byte[] keyVal = String.format("row_%012d", key).getBytes();
                Put put = new Put(keyVal);
                for(int c=0; c<numFields; c++)
                  put.add(columnFamily,qualifiers[c], String.format("cvl_%02d_%012d", c+1, sec).getBytes());

                startTime = System.nanoTime();
                htable.put(put);
                totalDuration += (System.nanoTime() - startTime)/1000;
            }
          //htable.flushCommits();
          htable.close();
        }catch(IOException  e){ LOG.error(e);}

        LOG.info("Thread " + threadId + " inserts completed Total time = " + totalDuration + " AVG time = " + totalDuration/recordsPerThread + " us.");
    }

    private void doReads()
    {
        LOG.info("Thread " + threadId + " starting reads");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);
        int mykeystart = threadId*recordsPerThread;
        int mykeyend   = (threadId+1)*recordsPerThread - 1;

        int mysecstart = mykeystart;
        int mysecend   = mykeystart + (recordsPerThread + selectivity - 1)/selectivity - 1;

        int readsPerThread = readNumRecords/numThreads;

        long totalDuration = 0;


        try
        {
            HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            //htable.setAutoFlushTo(true);
            long startTime = 0;
            //for (int i=0,key=mykeystart; i<selectivity; i++)
                for(int sec=mysecstart; sec<=mysecend; sec += (mysecend-mysecstart+1)/readsPerThread)
                {
                    Filter filter = new SingleColumnValueFilter(columns[readField-1].getFamily(), columns[readField-1].getQualifier()
                                    , CompareOp.EQUAL,String.format("cvl_%02d_%012d", readField, sec).getBytes());
                    Scan scan = new Scan();
                    if(projectField == 0)
                        scan.addFamily(columnFamily);
                    else
                    {
                        if(readField != projectField)
                            scan.addColumn(columnFamily, columns[readField - 1].getQualifier());

                        scan.addColumn(columnFamily, columns[projectField - 1].getQualifier());
                    }
                    scan.setFilter(filter);

                    //LOG.info("Filter = " + filter + " scan = " + scan);

                    startTime = System.nanoTime();

                    ResultScanner scanner = htable.getScanner(scan);
                    Result[] results = scanner.next(selectivity);

                    if(printResult)
                    {
                      LOG.info("query on " + String.format("cvl_%02d_%012d", readField, sec) + " returned " + results.length + " records ");
                      printResults(results);
                    }

                    //for(int r=0; r<results.length; r++)

                    totalDuration += (System.nanoTime() - startTime)/1000;
                }
            //htable.flushCommits();
            htable.close();
        }catch(IOException  e){ LOG.error(e);}

        LOG.info("Thread " + threadId + " reads completed Total time = " + totalDuration + " AVG time = " + totalDuration/readsPerThread + " us.");
    }

    public TCWrite01(int threadid)
    {
        this.threadId = threadid;
    }

    private static void printResults(Result[] results)
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

    private static void setProperties(String args[])
    {

        LOG.info("Listing properties ...");
        properties.list(System.out);
        numRecords = Integer.parseInt(properties.getProperty("NUMRECORDS", ""+NUMRECORDS));
        numReads = Integer.parseInt(properties.getProperty("NUMREADS", ""+NUMREADS));
        selectivity = Integer.parseInt(properties.getProperty("SELECTIVITY", ""+SELECTIVITY));
        numThreads = Integer.parseInt(properties.getProperty("NUMTHREADS", ""+NUMTHREADS));

        namespace = properties.getProperty("NAMESPACE", NAMESPACE);
        tablename = properties.getProperty("TABLENAME", TABLENAME);
        numFields = Integer.parseInt(properties.getProperty("NUMFIELDS", ""+NUMFIELDS));
        columnFamily = properties.getProperty("COLUMNFAMILY", COLUMNFAMILY).getBytes();

        readField = Integer.parseInt(properties.getProperty("READFIELD", ""+READFIELD));
        readNumRecords = Integer.parseInt(properties.getProperty("READNUMRECORDS", ""+READNUMRECORDS));
        projectField = Integer.parseInt(properties.getProperty("PROJECTFIELD", ""+PROJECTFIELD));
        printResult = Boolean.parseBoolean(properties.getProperty("PRINTRESULT", "false"));

        switch(properties.getProperty("TESTTYPE"))
        {
            case "READ": testType = TestType.READ; break;
            case "UPDATE": testType = TestType.UPDATE; break;
            case "DELETE": testType = TestType.DELETE; break;
            case "INSERT":default: testType = TestType.INSERT; break;
        }
        LOG.info("testType = " + testType);

    }

    public static void setColumns()
    {
        qualifiers = new byte[numFields][];
        columns = new Column[numFields];
        for(int i=0; i<numFields; i++)
        {
          qualifiers[i] = String.format("c_%02d", i+1).getBytes();
          columns[i] = new Column(columnFamily,qualifiers[i]);
        }
    }

    public static void createTable() throws Exception
    {
        HTableDescriptor td = new HTableDescriptor(TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
        td.addFamily(new HColumnDescriptor(columnFamily));
        admin.createTable(td);
    }

    public static void main(String args[]) throws Exception
    {

        LOG.info("Starting main ...");

        setProperties(args);
        conf = HBaseConfiguration.create();
        admin = new HBaseAdmin(conf);

        boolean tableExists = admin.tableExists(TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
        LOG.info("Table exists = " + tableExists);

        if(testType == TestType.INSERT)
        {
            if (tableExists)
            {
              admin.disableTable(TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
              admin.deleteTable(TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            }

            setColumns();
            createTable();
        }
        else if(testType == TestType.READ)
        {
          setColumns();
        }

        Thread threads[] = new Thread[numThreads];

        for(int i=0; i<numThreads; i++)
            threads[i] = new TCWrite01(i);

        for(int i=0; i<numThreads; i++)
            threads[i].start();

        waitForThreads(threads);

        admin.close();
        LOG.info("Ending main ...");

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
}
