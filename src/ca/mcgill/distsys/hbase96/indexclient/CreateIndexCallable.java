package ca.mcgill.distsys.hbase96.indexclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.google.protobuf.ByteString;


import ca.mcgill.distsys.hbase96.indexcommons.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommons.Util;
import ca.mcgill.distsys.hbase96.indexcommons.exceptions.InvalidQueryException;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorCreateRequest;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorCreateResponse;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.IndexCoprocessorInMemService;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.protobuf.generated.IndexCoprocessorInMem.ProtoColumn;

public class CreateIndexCallable implements Batch.Call<IndexCoprocessorInMemService, Boolean> {

	private IndexCoprocessorCreateRequest request;


	public CreateIndexCallable(List<Column> colList, String indexType,
			Object[] arguments)
	throws InvalidQueryException {

		IndexCoprocessorCreateRequest.Builder builder =
				IndexCoprocessorCreateRequest.newBuilder();

		//builder.setIsMultiColumns(true);
		for (Column queryCol : colList) {
			ProtoColumn.Builder columnBuilder = ProtoColumn.newBuilder();

			if (queryCol.getFamily() == null) {
				throw new InvalidQueryException(
						"Invalid Column in the query, a column MUST have a family.");
			}

			columnBuilder.setFamily(ByteString.copyFrom(queryCol.getFamily()));
			if (queryCol.getQualifier() != null) {
				columnBuilder.setQualifier(
						ByteString.copyFrom(queryCol.getQualifier()));
			}
			builder.addColumn(columnBuilder.build());
		}

		builder.setIndexType(indexType);

		List<ByteString> bytesArguments = new ArrayList<ByteString>();
		for (int i = 0; i < arguments.length; i++) {
			try {
				bytesArguments.add(
						ByteString.copyFrom(Util.serialize(arguments[i])));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		builder.addAllArguments(bytesArguments);

		request = builder.build();
	}

	public Boolean call(IndexCoprocessorInMemService instance)
	throws IOException {
		ServerRpcController controller = new ServerRpcController();
		BlockingRpcCallback<IndexCoprocessorCreateResponse> rpcCallback =
				new BlockingRpcCallback<IndexCoprocessorCreateResponse>();
		instance.createIndex(controller, request, rpcCallback);

		IndexCoprocessorCreateResponse response = rpcCallback.get();
		if (controller.failedOnException()) {
			throw controller.getFailedOn();
		}

		return response.getSuccess();
	}
}