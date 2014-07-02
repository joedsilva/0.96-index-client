package ca.mcgill.distsys.hbase96.inmemindexedclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.google.protobuf.ByteString;


import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.Util;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.exceptions.InvalidQueryException;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorCreateRequest;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorCreateResponse;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorInMemService;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.protobuf.generated.IndexCoprocessorInMem.ProtoColumn;

public class CreateIndexCallable implements Batch.Call<IndexCoprocessorInMemService, Boolean> {

    private IndexCoprocessorCreateRequest request;

    
    public CreateIndexCallable(List<Column> colList, String indexType, Object[] arguments, Class<?>[] argumentsClasses) throws InvalidQueryException {
    	IndexCoprocessorCreateRequest.Builder builder = IndexCoprocessorCreateRequest.newBuilder();
    	builder.setIsMultiColumns(true);
    	builder.setIndexType(indexType);
    	
    	List<ByteString> bytesArguments = new ArrayList<ByteString>();
    	List<ByteString> bytesArgumentsClasses = new ArrayList<ByteString>();
        for(int i = 0; i < arguments.length; i++){
      	  try {
  			bytesArguments.add(ByteString.copyFrom(Util.serialize(arguments[i])));
  			bytesArgumentsClasses.add(ByteString.copyFrom(Util.serialize(argumentsClasses[i])));
  		  } catch (IOException e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		  }
        }
        
        builder.addAllArguments(bytesArguments);
        builder.addAllArgumentsClasses(bytesArgumentsClasses);
        
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
    
    
    // Modified by Cong
    public CreateIndexCallable(byte[] family, byte[] qualifier, String indexType, Object[] arguments, Class<?>[] argumentsClasses) {
      IndexCoprocessorCreateRequest.Builder builder = IndexCoprocessorCreateRequest.newBuilder();
      builder.setFamily(ByteString.copyFrom(family));
      builder.setQualifier(ByteString.copyFrom(qualifier));
      builder.setIsMultiColumns(false);
      builder.setIndexType(indexType);
      
      List<ByteString> bytesArguments = new ArrayList<ByteString>();
      List<ByteString> bytesArgumentsClasses = new ArrayList<ByteString>();
      for(int i = 0; i < arguments.length; i++){
    	  try {
			bytesArguments.add(ByteString.copyFrom(Util.serialize(arguments[i])));
			bytesArgumentsClasses.add(ByteString.copyFrom(Util.serialize(argumentsClasses[i])));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
      }
      
      builder.addAllArguments(bytesArguments);
      builder.addAllArgumentsClasses(bytesArgumentsClasses);
      
      
      request = builder.build();
    }

    public Boolean call(IndexCoprocessorInMemService instance) throws IOException {
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<IndexCoprocessorCreateResponse> rpcCallback = new BlockingRpcCallback<IndexCoprocessorCreateResponse>();
        instance.createIndex(controller, request, rpcCallback);

        IndexCoprocessorCreateResponse response = rpcCallback.get();
        if (controller.failedOnException()) {
          throw controller.getFailedOn();
        }
        
        return response.getSuccess();
    }

}