package ca.mcgill.distsys.hbase96.indexclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import ca.mcgill.distsys.hbase96.indexcommons.Util;
import ca.mcgill.distsys.hbase96.indexcommons.exceptions.InvalidQueryException;
import ca.mcgill.distsys.hbase96.indexcommons.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorInMemService;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexedQueryRequest;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexedQueryResponse;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.ProtoResult;

public class IndexedQueryCallable implements Batch.Call<IndexCoprocessorInMemService, List<Result>> {

    private IndexedQueryRequest indexQuery;

    public IndexedQueryCallable(IndexedColumnQuery query) throws InvalidQueryException {
        indexQuery = Util.buildQuery(query);
    }

    public List<Result> call(IndexCoprocessorInMemService instance) throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<IndexedQueryResponse> rpcCallback = new BlockingRpcCallback<IndexedQueryResponse>();
        instance.execIndexedQuery(controller, indexQuery, rpcCallback);

        IndexedQueryResponse response = rpcCallback.get();
        if (controller.failedOnException()) {
            throw controller.getFailedOn();
        }

        if (response.getResultCount() > 0) {
            List<ProtoResult> protoResultList = response.getResultList();
            List<Result> result; // = new ArrayList<Result>(protoResultList.size());
            result = Util.toResults(protoResultList);

            return result;
        }
        return new ArrayList<Result>(0);
        
    }

}
