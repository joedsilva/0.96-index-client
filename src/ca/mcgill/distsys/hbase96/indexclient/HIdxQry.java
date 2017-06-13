package ca.mcgill.distsys.hbase96.indexclient;

import ca.mcgill.distsys.hbase96.indexcommons.proto.ByteArrayCriterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;
import ca.mcgill.distsys.hbase96.indexcommons.proto.IndexedColumnQuery;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

import static ca.mcgill.distsys.hbase96.indexclient.TCBase.readField;

/**
 * Created by jdsilv2 on 15/06/16.
 */
public class HIdxQry
{
    /** Static fields **/
    protected static Log LOG = LogFactory.getLog(TCBase.class);

    protected Configuration conf;
    protected HBaseAdmin admin;

    protected HIndexedTable idxTable;

    public HIdxQry(String tablename) throws IOException
    {
        conf = HBaseConfiguration.create();
        admin = new HBaseAdmin(conf);
        idxTable = new HIndexedTable(conf, TableName.valueOf(tablename.getBytes()).getNameAsString());
    }

    protected static void printResults(List<Result> results)
    {
        System.out.println(results.size() + " records returned from query.");
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

    public List<Result> queryTableWithIDx(String idxColumnFamily, String idxColumnName, String idxColumnValue, String outputColumnFamily, String outputColumn)
            throws Throwable
    {

        Criterion<?> criterion = new ByteArrayCriterion(new Column(idxColumnFamily, idxColumnName), idxColumnValue.getBytes());
        IndexedColumnQuery query = new IndexedColumnQuery(criterion);
        query.addColumn(new Column(outputColumnFamily, outputColumn));
        return idxTable.execIndexedQuery(query);
    }

    public static void main(String args[]) throws IOException, Throwable
    {
        Properties properties = System.getProperties();

        String tablename = properties.getProperty("TABLENAME");
        String columnFamily = properties.getProperty("COLUMNFAMILY");
        String idxField = properties.getProperty("IDXFIELD");
        String idxValue = properties.getProperty("IDXVALUE");
        String projectField = properties.getProperty("PROJECTFIELD");

        HIdxQry idxQry = new HIdxQry(tablename);

        printResults(idxQry.queryTableWithIDx(columnFamily, idxField, idxValue, columnFamily, projectField ));

    }

    protected void finalize()
    {
        try { idxTable.close(); admin.close(); } catch (IOException e){e.printStackTrace();}
    }

}
