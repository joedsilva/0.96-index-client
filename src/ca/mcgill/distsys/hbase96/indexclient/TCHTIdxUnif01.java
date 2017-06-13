package ca.mcgill.distsys.hbase96.indexclient;

import ca.mcgill.distsys.hbase96.indexcommons.proto.ByteArrayCriterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.IndexedColumnQuery;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by jdsilv2 on 16/08/16.
 */
public class TCHTIdxUnif01 extends  TCUnif01
{
    protected static Log LOG = LogFactory.getLog(TCUnif01.class);

    public TCHTIdxUnif01(int threadid)
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
        } catch (Throwable t){ LOG.error("Error while creating index.", t); throw  new Exception(t); }
    }

    protected static void createIndex() throws Throwable
    {
        Object[] htIdxarguments = null;

        if(numSIRegions > 1)
        {
            int recordsPerRegion = maxSIVal / numSIRegions;
            byte[] startKey = String.format("cvl_%02d_%010d_i", indexField, recordsPerRegion).getBytes();
            if(numSIRegions==2)
            {
                htIdxarguments = (Object[]) new byte[1][];
                ((byte[][])htIdxarguments)[0] = startKey;
            }
            else
            {
                byte[] endKey   = String.format("cvl_%02d_%010d_i", indexField, (maxSIVal - recordsPerRegion)).getBytes();
                htIdxarguments = new Object[3];
                htIdxarguments[0] = startKey;
                htIdxarguments[1] = endKey;
                htIdxarguments[2] = numSIRegions;
            }
        }
        idxTable.createHTableIndex(columns[indexField-1], htIdxarguments);
    }

    public static void dropIndex() throws Throwable
    { idxTable.deleteIndex(columns[indexField-1]); }

    @Override
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
        int totalResultSize = 0;

        try
        {
            HIndexedTable readidxTable = new HIndexedTable(conf, TableName.valueOf(tablename.getBytes()).getNameAsString());
            readidxTable.setAutoFlushTo(true);
            long startTime = 0, endTime = 0;
            Random rand = new Random();

            ArrayList projectFields = new ArrayList();
            if(projectField == 0)
            {    for(int p=0; p<columns.length; p++)  projectFields.add(columns[p]); }
            else
            {
                if(readField != projectField)
                    projectFields.add(columns[readField - 1]);
                projectFields.add( columns[projectField - 1]);
            }

            //for(int iter=0, sec=mykeystart+rand.nextInt(secValuesPerThread) ; iter<readsPerThread+readExtraSkipRecords; iter++, sec = mykeystart+rand.nextInt(secValuesPerThread))
            for(int iter=0, sec=rand.nextInt(maxSIVal) ; iter<readsPerThread+readExtraSkipRecords; iter++, sec = rand.nextInt(maxSIVal))
            {
                byte[] value = String.format("cvl_%02d_%010d_i", readField, sec).getBytes();


                startTime = System.nanoTime();
                Result[] results = readidxTable.getBySecondaryIndex(columns[readField-1].getFamily(), columns[readField-1].getQualifier() ,value,projectFields);
                endTime =  System.nanoTime();

                if(iter >= readExtraSkipRecords)
                    totalDuration += (endTime - startTime)/1000;
                else continue;

                totalResultSize += (results==null ? 0 : results.length);

                if(printResult)
                {
                    if(results != null)
                    {
                        LOG.info("query on " + String.format("cvl_%02d_%010d_i", readField, sec) + " returned " + results.length + " records ");
                        printResults(results);
                    }
                    else
                        LOG.info("query on " + String.format("cvl_%02d_%010d_i", readField, sec) + " returned " + 0 + " records ");
                }

            }
            readidxTable.flushCommits();
            readidxTable.close();
        }catch(IOException e){ e.printStackTrace(); LOG.error(e);}catch(Throwable t){t.printStackTrace(); LOG.error(t);}

        LOG.info("Thread " + threadId + " reads completed, Total records read = " + totalResultSize + " Total time = " + totalDuration + " AVG time = " + totalDuration/readsPerThread + " us.");
        //LOG.info("Thread " + threadId + " reads completed Total time = " + totalDuration + " AVG time = " + totalDuration/readsPerThread + " us.");
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
            HIndexedTable readidxTable = new HIndexedTable(conf, TableName.valueOf(tablename.getBytes()).getNameAsString());
            readidxTable.setAutoFlushTo(true);
            Random rand = new Random();
            long startTime = 0, duration=0;
            //for (int i=0,key=mykeystart; i<selectivity; i++)

            ArrayList projectFields = new ArrayList();
            if(projectField == 0)
            {    for(int p=0; p<columns.length; p++) if(p != readField-1)  projectFields.add(columns[p]); }
            else
            {
                if(readField != projectField)
                    projectFields.add(columns[readField - 1]);
                projectFields.add( columns[projectField - 1]);
            }

            for(int i=0; i<rangesPerThread; i++)
            {
                //int startRange = rand.nextInt(numRecords), endRange = rand.nextInt(numRecords), tmp=0;
                //if(startRange > endRange){ tmp=startRange;startRange=endRange;endRange=startRange;}
                int startRange = rand.nextInt(numRecords), endRange = startRange + rand.nextInt(maxRange);
                if(endRange > maxSIVal) endRange = maxSIVal;

                startTime = System.nanoTime();

                Result[] results = readidxTable.getBySecondaryIndexRange(columns[readField-1].getFamily(), columns[readField-1].getQualifier()
                        ,String.format("cvl_%02d_%010d_i", rangeField, startRange).getBytes(),String.format("cvl_%02d_%010d_i", rangeField, endRange).getBytes()
                        ,projectFields,resultBufferSize);

                if(printResult)
                {
                    LOG.info("query on " + String.format("cvl_%02d_%010d_i", rangeField, startRange) + ","
                            + String.format("cvl_%02d_%010d_i", rangeField, endRange) + " returned " + (results==null ? 0 : results.length) + " records ");
                    printResults(results);
                }

                //for(int r=0; r<results.length; r++)

                duration = (System.nanoTime() - startTime)/1000;
                totalDuration += duration;
                LOG.info("Thread " + threadId + " range read : range " + (endRange-startRange+1) + " records " + (results==null ? 0 : results.length)  + " duration " + duration + " us.");
            }
            readidxTable.flushCommits();
            readidxTable.close();
        }catch(IOException  e){ LOG.error(e);}catch(Throwable t){ LOG.error(t);}

        LOG.info("Thread " + threadId + " range reads completed Total time = " + totalDuration + " AVG time = " + totalDuration/rangesPerThread + " us.");
    }

    public static void main(String args[]) throws Exception
    {

        LOG.info("Starting main ...");
        initialize(args);
        if(createDDLOnly)
        {
            LOG.info("Ending main ... create DDL only specified");
            return;
        }

        //siRandom = new Random();
        //keyRandom = new Random();

        Thread threads[] = new Thread[numThreads];

        for(int i=0; i<numThreads; i++)
            threads[i] = new TCHTIdxUnif01(i);

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
