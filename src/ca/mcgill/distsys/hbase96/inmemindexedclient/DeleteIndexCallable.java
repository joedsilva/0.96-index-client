package ca.mcgill.distsys.hbase96.inmemindexedclient;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorDeleteRequest;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorDeleteResponse;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorInMemService;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.ProtoColumn;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.exceptions.InvalidQueryException;

import com.google.protobuf.ByteString;

public class DeleteIndexCallable implements Batch.Call<IndexCoprocessorInMemService, Boolean>{
    private IndexCoprocessorDeleteRequest request;

    public DeleteIndexCallable(List<Column> colList) throws InvalidQueryException {
        IndexCoprocessorDeleteRequest.Builder builder = IndexCoprocessorDeleteRequest.newBuilder();
        //builder.setFamily(ByteString.copyFrom(family));
        //builder.setQualifier(ByteString.copyFrom(qualifier));
        builder.setIsMultiColumns(true);
        
        for (Column queryCol : colList) {
            ProtoColumn.Builder columnBuilder = ProtoColumn.newBuilder();

            if (queryCol.getFamily() == null) {
                throw new InvalidQueryException("Invalid Column in the query, a column MUST have a family.");
            }

            columnBuilder.setFamily(ByteString.copyFrom(queryCol.getFamily()));
            if (queryCol.getQualifier() != null) {
                columnBuilder.setQualifier(ByteString.copyFrom(queryCol.getQualifier()));
            }
            builder.addColumn(columnBuilder.build());
        }

        request = builder.build();
    }
    
    public DeleteIndexCallable(byte[] family, byte[] qualifier) {
        IndexCoprocessorDeleteRequest.Builder builder = IndexCoprocessorDeleteRequest.newBuilder();
        builder.setFamily(ByteString.copyFrom(family));
        builder.setQualifier(ByteString.copyFrom(qualifier));
        

        request = builder.build();
    }

    public Boolean call(IndexCoprocessorInMemService instance) throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<IndexCoprocessorDeleteResponse> rpcCallback = new BlockingRpcCallback<IndexCoprocessorDeleteResponse>();
        instance.deleteIndex(controller, request, rpcCallback);

        IndexCoprocessorDeleteResponse response = rpcCallback.get();
        if (controller.failedOnException()) {
            throw controller.getFailedOn();
        }

        return response.getSuccess();
    }
}
