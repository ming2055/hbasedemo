package com.hbase.coprocessor.server;

import com.hbase.coprocessor.generated.DataQueryProtos;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SlatTableDataSearch extends DataQueryProtos.QueryDataService implements Coprocessor, CoprocessorService {
    private RegionCoprocessorEnvironment env;

    public SlatTableDataSearch() {
    }

    /**
     * Just returns a reference to this object, which implements the QueryDataService interface.
     */
    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void queryByStartRowAndEndRow(RpcController controller,
                                         DataQueryProtos.DataQueryRequest request,
                                         RpcCallback<DataQueryProtos.DataQueryResponse> done) {
        DataQueryProtos.DataQueryResponse response = null;

        String startRow = request.getStartRow();
        String endRow = request.getEndRow();
        // a b c
        String regionStartKey = Bytes.toString(this.env.getRegion().getRegionInfo().getStartKey());

        if (request.getIsSalting()) {
            String startSalt = null;
            if (null != regionStartKey && regionStartKey.length() != 0) {
                startSalt = regionStartKey;
            }
            if (null != startSalt && null != startRow) {
                //在startRow与endRow前面加上salt
                startRow = startSalt + "-" + startRow;
                endRow = startSalt + "-" + endRow;
            }
        }

        //正常的scan查询
        Scan scan = new Scan();
        if (null != startRow) {
            scan.setStartRow(Bytes.toBytes(startRow));
//            scan.withStartRow(Bytes.toBytes(startRow));
        }

        if (null != endRow) {
            scan.setStopRow(Bytes.toBytes(endRow));
//            scan.withStopRow(Bytes.toBytes(endRow), request.getIncluedEnd());
        }

        try (InternalScanner scanner = this.env.getRegion().getScanner(scan)) {
            List<Cell> results = new ArrayList<>();

            boolean hasMore;
            DataQueryProtos.DataQueryResponse.Builder responseBuilder = DataQueryProtos.DataQueryResponse.newBuilder();
            do {
                hasMore = scanner.next(results);
                DataQueryProtos.DataQueryResponse.Row.Builder rowBuilder = DataQueryProtos.DataQueryResponse.Row.newBuilder();
                if (results.size() > 0) {
                    Cell cell = results.get(0);
                    rowBuilder.setRowKey(ByteString.copyFrom(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                    for (Cell kv : results) {
                        buildCell(rowBuilder, kv);
                    }
                }

                responseBuilder.addRowList(rowBuilder);
                results.clear();
            } while (hasMore);

            response = responseBuilder.build();

        } catch (IOException e) {
            ResponseConverter.setControllerException(controller, e);
        }
        done.run(response);
    }

    private void buildCell(DataQueryProtos.DataQueryResponse.Row.Builder rowBuilder, Cell kv) {
        DataQueryProtos.DataQueryResponse.Cell.Builder cellBuilder = DataQueryProtos.DataQueryResponse.Cell.newBuilder();
        cellBuilder.setFamily(ByteString.copyFrom(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength()));
        cellBuilder.setQualifier(ByteString.copyFrom(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()));
        cellBuilder.setRow(ByteString.copyFrom(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()));
        cellBuilder.setValue(ByteString.copyFrom(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
        cellBuilder.setTimestamp(kv.getTimestamp());
        rowBuilder.addCellList(cellBuilder);
    }

    /**
     * Stores a reference to the coprocessor environment provided by the
     * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
     * coprocessor is loaded.  Since this is a coprocessor endpoint, it always expects to be loaded
     * on a table region, so always expects this to be an instance of
     * {@link RegionCoprocessorEnvironment}.
     *
     * @param env the environment provided by the coprocessor host
     * @throws IOException if the provided environment is not an instance of
     *                     {@code RegionCoprocessorEnvironment}
     */
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        // nothing to do
    }


}
