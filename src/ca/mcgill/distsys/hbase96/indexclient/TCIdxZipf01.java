package ca.mcgill.distsys.hbase96.indexclient;

import ca.mcgill.distsys.hbase96.indexcommons.proto.ByteArrayCriterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.IndexedColumnQuery;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Created by jdsilv2 on 14/06/16.
 */
public class TCIdxZipf01 extends TCZipf01
{

    protected static Log LOG = LogFactory.getLog(TCIdxZipf01.class);

    public TCIdxZipf01(int threadid)
    {
        super(threadid);
    }

    protected static void initialize(String args[]) throws Exception
    {
        TCBase.initialize(args);
        idxTable = new HIndexedTable(conf, TableName.valueOf(tablename.getBytes()).getNameAsString());

        try
        {
            if(createIndex)
            {
                try { dropIndex(); } catch (Throwable t){ LOG.warn(t); }
                createIndex();
            }
        } catch (Throwable t){ throw  new Exception(t); }
    }

    protected static void createIndex() throws Throwable
    {
        idxTable.createHybridIndex2(columns[indexField-1]);
    }

    public static void dropIndex() throws Throwable
    {
        idxTable.deleteIndex(columns[indexField-1]);
    }


    @Override
    protected void doReads()
    {
        LOG.info("Thread " + threadId + " starting reads");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);
        int mykeystart = threadId*recordsPerThread;
        //int mykeyend   = (threadId+1)*recordsPerThread - 1;

        //int mysecstart = mykeystart;
        //int mysecend   = mykeystart + (recordsPerThread + selectivity - 1)/selectivity - 1;

        int readsPerThread = readNumRecords/numThreads;

        int totalResultSize = 0;
        long totalDuration = 0;

        try
        {
            HIndexedTable readidxTable = new HIndexedTable(conf, TableName.valueOf(tablename.getBytes()).getNameAsString());
            readidxTable.setAutoFlushTo(true);
            long startTime = 0, endTime = 0;
            //for (int i=0,key=mykeystart; i<selectivity; i++)
            for(int iter=0, sec=zg.nextValue().intValue(); iter<readsPerThread+readExtraSkipRecords; iter++,sec=zg.nextValue().intValue())
            {
                Criterion<?> criterion = new ByteArrayCriterion(columns[readField-1], String.format("cvl_%02d_%010d_i", readField, sec).getBytes());
                IndexedColumnQuery query = new IndexedColumnQuery(criterion);

                if(projectField == 0)
                {    for(int p=0; p<columns.length; p++) if(p != readField-1) query.addColumn(columns[p]); }
                else
                {
                    if(readField != projectField)
                        query.addColumn(columns[readField - 1]);

                    query.addColumn( columns[projectField - 1]);
                }

                //LOG.info("Query = " + query + " Criterion = " + criterion + " column list " + query.getColumnList());

                startTime = System.nanoTime();
                List<Result> results = readidxTable.execIndexedQuery(query);
                endTime = System.nanoTime();

                if(iter >= readExtraSkipRecords)
                    totalDuration += (endTime - startTime)/1000;

                totalResultSize += results.size();

                if(printResult)
                {
                    LOG.info("query on " + String.format("cvl_%02d_%010d_i", readField, sec) + " returned " + results.size() + " records ");
                    printResults(results);
                }


            }
            readidxTable.flushCommits();
            readidxTable.close();
        }catch(IOException e){ LOG.error(e);} catch(Throwable t){ LOG.error(t);}

        LOG.info("Thread " + threadId + " reads completed, Total records read = " + totalResultSize + " Total time = " + totalDuration + " AVG time = " + totalDuration/readsPerThread + " us.");
    }

    @Override
    protected void doRangeReads()
    {
        LOG.info("Thread " + threadId + " starting range queries");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);
        int mykeystart = threadId*recordsPerThread;
        //int mykeyend   = (threadId+1)*recordsPerThread - 1;

        //int mysecstart = mykeystart;
        //int mysecend   = mykeystart + (recordsPerThread + selectivity - 1)/selectivity - 1;

        int rangesPerThread = numRanges/numThreads;

        int totalResultSize = 0;
        long totalDuration = 0;

        try
        {
            //HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            HIndexedTable readidxTable = new HIndexedTable(conf, TableName.valueOf(tablename.getBytes()).getNameAsString());
            readidxTable.setAutoFlushTo(true);
            Random rand = new Random();
            long startTime = 0, duration=0;
            //for (int i=0,key=mykeystart; i<selectivity; i++)
            //for(int iter=0, sec=zg.nextValue().intValue(); iter<readsPerThread; iter++,sec=zg.nextValue().intValue())
            for(int i=0; i<rangesPerThread; i++)
            {
                //int startRange = zg.nextValue().intValue(), endRange = zg.nextValue().intValue(), tmp=0;
                //if(startRange > endRange){ tmp=startRange;startRange=endRange;endRange=startRange;}
                int startRange = zg.nextValue().intValue(), endRange = startRange + rand.nextInt(maxRange);
                if(endRange > maxSIVal) endRange = maxSIVal;


                Criterion<?> criterion = new ByteArrayCriterion(columns[readField-1]
                        , String.format("cvl_%02d_%010d_i", rangeField, startRange).getBytes()
                        , String.format("cvl_%02d_%010d_i", rangeField, endRange).getBytes());
                IndexedColumnQuery query = new IndexedColumnQuery(criterion);

                if(projectField == 0)
                {    for(int p=0; p<columns.length; p++) if(p != rangeField-1) query.addColumn(columns[p]); }
                else
                {
                    if(rangeField != projectField)
                        query.addColumn(columns[rangeField - 1]);

                    query.addColumn( columns[projectField - 1]);
                }

                //LOG.info("Filter = " + filter + " scan = " + scan);

                startTime = System.nanoTime();

                List<Result> results = readidxTable.execIndexedQuery(query);
                totalResultSize += results.size();

                if(printResult)
                {
                    LOG.info("query on " + String.format("cvl_%02d_%010d_i", rangeField, startRange) + ","
                            + String.format("cvl_%02d_%010d_i", rangeField, endRange) + " returned " + results.size() + " records ");
                    printResults(results);
                }

                //for(int r=0; r<results.length; r++)

                duration = (System.nanoTime() - startTime)/1000;
                totalDuration += duration;
                LOG.info("Thread " + threadId + " range read : range_("+endRange+"-"+startRange+") " + (endRange-startRange+1) + " records " + results.size() + " duration " + duration + " us.");
            }
            readidxTable.flushCommits();
            readidxTable.close();
        }catch(IOException  e){ LOG.error(e);} catch(Throwable t){ LOG.error(t);}

        LOG.info("Thread " + threadId + " range reads completed, Total records read = " + totalResultSize + "  Total time = " + totalDuration + " AVG time = " + totalDuration/rangesPerThread + " us.");

    }


    public static void main(String args[]) throws Exception
    {

        LOG.info("Starting main ...");
        initialize(args);

        zg = new ScrambledZipfianGenerator(0,numRecords);
        Thread threads[] = new Thread[numThreads];

        for(int i=0; i<numThreads; i++)
            threads[i] = new TCIdxZipf01(i);

        for(int i=0; i<numThreads; i++)
            threads[i].start();

        waitForThreads(threads);

        cleanup();
        LOG.info("Ending main ...");

    }

    protected static void cleanup() throws Exception
    {
        idxTable.close();
        TCUnif01.cleanup();
    }

}
