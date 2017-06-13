package ca.mcgill.distsys.hbase96.indexclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.List;

/**
 * Created by joseph on 27/05/16.
 */
public class TCUnif01 extends TCBase
{
    protected static Log LOG = LogFactory.getLog(TCUnif01.class);

    public TCUnif01(int threadid)
    {
        super(threadid);
    }


    protected void doInserts()
    {
        LOG.info("Thread " + threadId + " starting inserts");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);

        long totalDuration = 0, timeLogIntervalDuration=0;

        try
        {
            //HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            HTable htable = new HTable(conf, TableName.valueOf(tablename.getBytes()));
            htable.setAutoFlushTo(true);
            long startTime = 0, iterDuration = 0;
            //for (int key=threadId, iter=1; key<numRecords; key+=numThreads, iter++)
            //for (int i=0, iter=1; i<recordsPerThread; i++, iter++)
            for (int i=0, iter=1, key=threadId; i<recordsPerThread; i++, iter++, key+=numThreads)
                //for(int sec=mysecstart; sec<=mysecend; sec++, key++)
                {
                    //LOG.info("Thread = " + threadId + " key = " + key + " sec = " + sec);

                    int inskey = key;
                    if(! rowkeySequential) inskey = keyRandom.nextInt((int)(rowkeyDistMultiplier *numRecords));
                    else if(rowkeysAlternateRegions) inskey = key/numRegions +  key%numRegions*recordsPerRegion;

                    //byte[] keyVal = String.format("row_%012d", keyRandom.nextInt(2*numRecords)).getBytes();
                    byte[] keyVal = String.format("row_%012d", inskey).getBytes();
                    Put put = new Put(keyVal);
                    for(int c=0; c<numFields; c++)
                        put.add(columnFamily,qualifiers[c], String.format("cvl_%02d_%010d_i", c+1, siRandom.nextInt(maxSIVal)).getBytes());

                    startTime = System.nanoTime();
                    htable.put(put);

                    iterDuration = (System.nanoTime() - startTime)/1000;
                    totalDuration += iterDuration;
                    timeLogIntervalDuration += iterDuration;

                    if(iter%timeLogRecords == 0)
                    {
                        LOG.info("Thread " + threadId + " inserts in progress, iter = " + iter + " interval time = " + timeLogIntervalDuration + " AVG_time = " + timeLogIntervalDuration/timeLogRecords + " us.");
                        timeLogIntervalDuration = 0;
                    }
                }
            htable.flushCommits();
            htable.close();
        }catch(IOException e){ e.printStackTrace(); LOG.error(e);}

        LOG.info("Thread " + threadId + " inserts completed Total time = " + totalDuration + " AVG time = " + totalDuration/recordsPerThread + " us.");
    }


    @Override
    protected void doBatchInserts()
    {
        LOG.info("Thread " + threadId + " starting inserts");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);

        long totalDuration = 0, timeLogIntervalDuration=0;
        long startTime = 0, iterDuration = 0;
        List<Row> batch = new ArrayList<Row>();

        try
        {
            //HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            HTable htable = new HTable(conf, TableName.valueOf(tablename.getBytes()));
            //htable.setAutoFlushTo(false);
            //for (int key=threadId, iter=1; key<numRecords; key+=numThreads, iter++)
            //for (int i=0, iter=1; i<recordsPerThread; i++, iter++)
            for (int i=0, iter=1, key=threadId; i<recordsPerThread; i++, iter++, key+=numThreads)
            //for(int sec=mysecstart; sec<=mysecend; sec++, key++)
            {
                //LOG.info("Thread = " + threadId + " key = " + key + " sec = " + sec);

                int inskey = key;
                if(! rowkeySequential) inskey = keyRandom.nextInt((int)(rowkeyDistMultiplier *numRecords));
                else if(rowkeysAlternateRegions) inskey = key/numRegions +  key%numRegions*recordsPerRegion;

                //byte[] keyVal = String.format("row_%012d", keyRandom.nextInt(2*numRecords)).getBytes();
                byte[] keyVal = String.format("row_%012d", inskey).getBytes();
                Put put = new Put(keyVal);
                for(int c=0; c<numFields; c++)
                    put.add(columnFamily,qualifiers[c], String.format("cvl_%02d_%010d_i", c+1, siRandom.nextInt(maxSIVal)).getBytes());

                // startTime = System.nanoTime();
                batch.add(put);

                //iterDuration = (System.nanoTime() - startTime)/1000;
                //totalDuration += iterDuration;
                //timeLogIntervalDuration += iterDuration;

                if(iter == timeLogRecords)
                {
                    startTime = System.nanoTime();
                    htable.batch(batch);
                    timeLogIntervalDuration = (System.nanoTime() - startTime)/1000;
                    LOG.info("Thread " + threadId + " inserts in progress, iter = " + iter + " time = " + timeLogIntervalDuration + " AVG_time = " + timeLogIntervalDuration/timeLogRecords + " us.");
                    batch = new ArrayList<Row>();
                }
                else if(iter%timeLogRecords == 0)
                {
                    startTime = System.nanoTime();
                    htable.batch(batch);
                    timeLogIntervalDuration = (System.nanoTime() - startTime)/1000;
                    totalDuration += timeLogIntervalDuration;
                    LOG.info("Thread " + threadId + " inserts in progress, iter = " + iter + " interval time = " + timeLogIntervalDuration + " AVG_time = " + timeLogIntervalDuration/timeLogRecords + " us.");
                    batch = new ArrayList<Row>();
                }
            }

            //startTime = System.nanoTime();
            //htable.batch(batch);
            //totalDuration = (System.nanoTime() - startTime)/1000;
            LOG.info("Thread " + threadId + " closing HTable." );
            //htable.flushCommits();
            htable.close();
        }catch(IOException | InterruptedException e){ LOG.error(e);}

        LOG.info("Thread " + threadId + " inserts completed Total time = " + totalDuration + " AVG time = " + totalDuration/recordsPerThread + " us.");
    }


    protected void doUpdates()
    {
        LOG.info("Thread " + threadId + " starting updates");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);
        int mykeystart = threadId*recordsPerThread;
        int mykeyend   = (threadId+1)*recordsPerThread - 1;

        int mysecstart = mykeystart;
        int mysecend   = mykeystart + (recordsPerThread + selectivity - 1)/selectivity - 1;

        int updatesPerThread = numUpdates/numThreads;
        LOG.info("Records per thread = " + recordsPerThread + " updates per thread = " + updatesPerThread);

        long totalDuration = 0;

        try
        {
            //we need to pick a row key at random as well as SI value.
            //Random rand = new Random(), rand2 = new Random();

            //HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            HTable htable = new HTable(conf, TableName.valueOf(tablename.getBytes()));
            htable.setAutoFlushTo(true);
            long startTime = 0, endTime = 0;
            for (int iter=0,key=siRandom.nextInt(numRecords); iter<updatesPerThread+numExtraUpdates; iter++, key=siRandom.nextInt(numRecords))
            {
              //LOG.info("Thread = " + threadId + " key = " + key + " sec = " + sec);

              byte[] keyVal = String.format("row_%012d", key).getBytes();
              Put put = new Put(keyVal);
              //put.add(columnFamily,qualifiers[updateField-1], String.format("cvl_%02d_%010d_u", updateField, mysecstart+siRandom.nextInt(maxSIVal)).getBytes());
              put.add(columnFamily,qualifiers[updateField-1], String.format("cvl_%02d_%010d_u", updateField, siRandom.nextInt(maxSIVal)).getBytes());

              startTime = System.nanoTime();
              htable.put(put);
              endTime = System.nanoTime();

              if(iter >= numExtraUpdates)
                totalDuration += (endTime - startTime)/1000;
            }
            htable.flushCommits();
            htable.close();
        }catch(IOException e){ LOG.error(e);}

        LOG.info("Thread " + threadId + " updates completed Total time = " + totalDuration + " AVG time = " + totalDuration/updatesPerThread + " us.");
    }


    protected void doBatchUpdates()
    {
        LOG.info("Thread " + threadId + " starting updates");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);
        int mykeystart = threadId*recordsPerThread;
        int mykeyend   = (threadId+1)*recordsPerThread - 1;

        int mysecstart = mykeystart;
        int mysecend   = mykeystart + (recordsPerThread + selectivity - 1)/selectivity - 1;

        int updatesPerThread = numUpdates/numThreads;
        LOG.info("Records per thread = " + recordsPerThread + " updates per thread = " + updatesPerThread);

        long totalDuration = 0, timeLogIntervalDuration=0;
        long startTime = 0, endTime = 0;
        List<Row> batch = new ArrayList<Row>();

        try
        {
            //we need to pick a row key at random as well as SI value.
            //Random rand = new Random(), rand2 = new Random();

            //HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            HTable htable = new HTable(conf, TableName.valueOf(tablename.getBytes()));
            //htable.setAutoFlushTo(true);
            for (int iter=1,key=siRandom.nextInt(numRecords); iter<=updatesPerThread+numExtraUpdates; iter++, key=siRandom.nextInt(numRecords))
            {
                //LOG.info("Thread = " + threadId + " key = " + key + " sec = " + sec);

                byte[] keyVal = String.format("row_%012d", key).getBytes();
                Put put = new Put(keyVal);
                //put.add(columnFamily,qualifiers[updateField-1], String.format("cvl_%02d_%010d_u", updateField, mysecstart+siRandom.nextInt(maxSIVal)).getBytes());
                put.add(columnFamily,qualifiers[updateField-1], String.format("cvl_%02d_%010d_u", updateField, siRandom.nextInt(maxSIVal)).getBytes());

                //startTime = System.nanoTime();
                batch.add(put);
                //endTime = System.nanoTime();

                if(iter == numExtraUpdates)
                {
                    startTime = System.nanoTime();
                    htable.batch(batch);
                    timeLogIntervalDuration = (System.nanoTime() - startTime)/1000;
                    LOG.info("Thread " + threadId + " updates in progress, iter = " + iter + " time = " + timeLogIntervalDuration + " AVG_time = " + timeLogIntervalDuration/numExtraUpdates + " us.");
                    batch = new ArrayList<Row>();
                }
                else if(iter%numExtraUpdates == 0)
                {
                    startTime = System.nanoTime();
                    htable.batch(batch);
                    timeLogIntervalDuration = (System.nanoTime() - startTime)/1000;
                    totalDuration += timeLogIntervalDuration;
                    LOG.info("Thread " + threadId + " updates in progress, iter = " + iter + " interval time = " + timeLogIntervalDuration + " AVG_time = " + timeLogIntervalDuration/numExtraUpdates + " us.");
                    batch = new ArrayList<Row>();
                }
            }
            //htable.flushCommits();
            LOG.info("Thread " + threadId + " closing HTable." );
            htable.close();
        }catch(IOException | InterruptedException e){ LOG.error(e);}

        LOG.info("Thread " + threadId + " updates completed Total time = " + totalDuration + " AVG time = " + totalDuration/updatesPerThread + " us.");
    }


    protected   void doReads()
    {
        LOG.info("Thread " + threadId + " starting reads");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);
        int mykeystart = threadId*recordsPerThread;
        int mykeyend   = (threadId+1)*recordsPerThread - 1;

        int mysecstart = mykeystart;
        int mysecend   = mykeystart + (recordsPerThread + selectivity - 1)/selectivity - 1;
        int secValuesPerThread = mysecend - mysecstart + 1;

        int readsPerThread = readNumRecords/numThreads;

        long totalDuration = 0;


        try
        {
            //HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            HTable htable = new HTable(conf, TableName.valueOf(tablename.getBytes()));
            htable.setAutoFlushTo(true);
            long startTime = 0;
            Random rand = new Random();

            //for (int i=0,key=mykeystart; i<selectivity; i++)
            //for(int sec=mysecstart; sec<=mysecend; sec += (mysecend-mysecstart+1)/readsPerThread)
            for(int iter=0, sec=mykeystart+rand.nextInt(secValuesPerThread) ; iter<readsPerThread; iter++, sec = mykeystart+rand.nextInt(secValuesPerThread))
            {
                Filter filter = new SingleColumnValueFilter(columns[readField-1].getFamily(), columns[readField-1].getQualifier()
                        , CompareFilter.CompareOp.EQUAL,String.format("cvl_%02d_%010d_i", readField, sec).getBytes());
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
                Result[] results = scanner.next(selectivity*10);

                if(printResult)
                {
                    LOG.info("query on " + String.format("cvl_%02d_%010d_i", readField, sec) + " returned " + results.length + " records ");
                    printResults(results);
                }

                //for(int r=0; r<results.length; r++)

                totalDuration += (System.nanoTime() - startTime)/1000;
            }
            htable.flushCommits();
            htable.close();
        }catch(IOException  e){ LOG.error(e);}

        LOG.info("Thread " + threadId + " reads completed Total time = " + totalDuration + " AVG time = " + totalDuration/readsPerThread + " us.");
    }

    @Override
    protected void doRangeReads()
    {
        LOG.info("Thread " + threadId + " starting range queries");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        LOG.info("Records per thread = " + recordsPerThread);
        int mykeystart = threadId*recordsPerThread;
        int mykeyend   = (threadId+1)*recordsPerThread - 1;

        int mysecstart = mykeystart;
        int mysecend   = mykeystart + (recordsPerThread + selectivity - 1)/selectivity - 1;

        int rangesPerThread = numRanges/numThreads;

        long totalDuration = 0;


        try
        {
            //HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            HTable htable = new HTable(conf, TableName.valueOf(tablename.getBytes()));
            htable.setAutoFlushTo(true);
            Random rand = new Random();
            long startTime = 0, duration=0;
            //for (int i=0,key=mykeystart; i<selectivity; i++)
            for(int i=0; i<rangesPerThread; i++)
            {
                //int startRange = rand.nextInt(numRecords), endRange = rand.nextInt(numRecords), tmp=0;
                //if(startRange > endRange){ tmp=startRange;startRange=endRange;endRange=startRange;}
                int startRange = rand.nextInt(numRecords), endRange = startRange + rand.nextInt(maxRange);
                if(endRange > maxSIVal) endRange = maxSIVal;

                FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                Filter f1 = new SingleColumnValueFilter(columns[rangeField-1].getFamily(), columns[rangeField-1].getQualifier()
                        , CompareFilter.CompareOp.GREATER_OR_EQUAL,String.format("cvl_%02d_%010d_i", rangeField, startRange).getBytes());
                Filter f2 = new SingleColumnValueFilter(columns[rangeField-1].getFamily(), columns[rangeField-1].getQualifier()
                        , CompareFilter.CompareOp.LESS_OR_EQUAL,String.format("cvl_%02d_%010d_i", rangeField, endRange).getBytes());
                filter.addFilter(f1);
                filter.addFilter(f2);

                Scan scan = new Scan();
                if(projectField == 0)
                    scan.addFamily(columnFamily);
                else
                {
                    if(rangeField != projectField)
                        scan.addColumn(columnFamily, columns[rangeField - 1].getQualifier());

                    scan.addColumn(columnFamily, columns[projectField - 1].getQualifier());
                }
                scan.setFilter(filter);

                //LOG.info("Filter = " + filter + " scan = " + scan);

                startTime = System.nanoTime();

                ResultScanner scanner = htable.getScanner(scan);
                Result[] results = scanner.next(endRange-startRange+1);

                if(printResult)
                {
                    LOG.info("query on " + String.format("cvl_%02d_%010d_i", rangeField, startRange) + ","
                                         + String.format("cvl_%02d_%010d_i", rangeField, endRange) + " returned " + results.length + " records ");
                    printResults(results);
                }

                //for(int r=0; r<results.length; r++)

                duration = (System.nanoTime() - startTime)/1000;
                totalDuration += duration;
                LOG.info("Thread " + threadId + " range read : range " + (endRange-startRange+1) + " records " + results.length + " duration " + duration + " us.");
            }
            htable.flushCommits();
            htable.close();
        }catch(IOException  e){ LOG.error(e);}

        LOG.info("Thread " + threadId + " range reads completed Total time = " + totalDuration + " AVG time = " + totalDuration/rangesPerThread + " us.");
    }

    @Override
    protected void doDeletes()
    {
        LOG.info("Thread " + threadId + " starting deletes");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        int deletesPerThread = numDeletes/numThreads;

        LOG.info("Records per thread = " + recordsPerThread + " deletes per thread = " + deletesPerThread);

        long totalDuration = 0;

        try
        {
            //we need to pick a row key at random.
            Random rand = new Random();

            //HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            HTable htable = new HTable(conf, TableName.valueOf(tablename.getBytes()));
            htable.setAutoFlushTo(true);
            long startTime = 0, endTime = 0;
            for (int iter=0, key=rand.nextInt(numRecords);iter<deletesPerThread+numExtraDeletes; iter++, key=rand.nextInt(numRecords))
            {
                byte[] keyVal = String.format("row_%012d", key).getBytes();
                Delete delete = new Delete(keyVal);

                startTime = System.nanoTime();
                htable.delete(delete);
                endTime = System.nanoTime();

                if(iter >= numExtraDeletes)
                    totalDuration += (endTime - startTime)/1000;
            }
            htable.flushCommits();
            htable.close();
        }catch(IOException e){ LOG.error(e);}

        LOG.info("Thread " + threadId + " deletes completed Total time = " + totalDuration + " AVG time = " + totalDuration/deletesPerThread + " us.");
    }

    @Override
    protected void doBatchDeletes()
    {
        LOG.info("Thread " + threadId + " starting deletes");

        int recordsPerThread = (numRecords+numThreads-1)/numThreads;
        int deletesPerThread = numDeletes/numThreads;

        LOG.info("Records per thread = " + recordsPerThread + " deletes per thread = " + deletesPerThread);

        long totalDuration = 0, timeLogIntervalDuration=0;
        long startTime = 0, endTime = 0;
        List<Row> batch = new ArrayList<Row>();

        try
        {
            //we need to pick a row key at random.
            Random rand = new Random();

            //HTable htable = new HTable(conf, TableName.valueOf(namespace.getBytes(),tablename.getBytes()));
            HTable htable = new HTable(conf, TableName.valueOf(tablename.getBytes()));
            //htable.setAutoFlushTo(true);
            for (int iter=1, key=rand.nextInt(numRecords);iter<=deletesPerThread+numExtraDeletes; iter++, key=rand.nextInt(numRecords))
            {
                byte[] keyVal = String.format("row_%012d", key).getBytes();
                Delete delete = new Delete(keyVal);
                batch.add(delete);

                if(iter == numExtraDeletes)
                {
                    startTime = System.nanoTime();
                    htable.batch(batch);
                    timeLogIntervalDuration = (System.nanoTime() - startTime)/1000;
                    LOG.info("Thread " + threadId + " deletes in progress, iter = " + iter + " time = " + timeLogIntervalDuration + " AVG_time = " + timeLogIntervalDuration/numExtraDeletes + " us.");
                    batch = new ArrayList<Row>();
                }
                else if(iter%numExtraDeletes == 0)
                {
                    startTime = System.nanoTime();
                    htable.batch(batch);
                    timeLogIntervalDuration = (System.nanoTime() - startTime)/1000;
                    totalDuration += timeLogIntervalDuration;
                    LOG.info("Thread " + threadId + " deletes in progress, iter = " + iter + " interval time = " + timeLogIntervalDuration + " AVG_time = " + timeLogIntervalDuration/numExtraDeletes + " us.");
                    batch = new ArrayList<Row>();
                }
            }
            //htable.flushCommits();
            LOG.info("Thread " + threadId + " closing HTable." );
            htable.close();
        }catch(IOException | InterruptedException e){ LOG.error(e);}

        LOG.info("Thread " + threadId + " deletes completed Total time = " + totalDuration + " AVG time = " + totalDuration/deletesPerThread + " us.");
    }

    public static void main(String args[]) throws Exception
    {

        LOG.info("Starting main ...");
        initialize(args);

        Thread threads[] = new Thread[numThreads];

        for(int i=0; i<numThreads; i++)
            threads[i] = new TCUnif01(i);

        for(int i=0; i<numThreads; i++)
            threads[i].start();

        waitForThreads(threads);

        cleanup();
        LOG.info("Ending main ...");

    }


}
