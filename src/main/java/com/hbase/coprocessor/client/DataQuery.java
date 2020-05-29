package com.hbase.coprocessor.client;

import com.hbase.coprocessor.generated.DataQueryProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DataQuery {

    public static Configuration conf = null;
    public static Connection connection;

    static {
        conf = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static List<DataQueryProtos.DataQueryResponse.Row> queryByStartRowAndStopRow(String tableName,
                                                                                         String startRow, String stopRow,
                                                                                         boolean isIncludeEnd, boolean isSalting) {

        final DataQueryProtos.DataQueryRequest.Builder requestBuilder = DataQueryProtos.DataQueryRequest.newBuilder();
        requestBuilder.setTableName(tableName);
        requestBuilder.setStartRow(startRow);
        requestBuilder.setEndRow(stopRow);
        requestBuilder.setIncluedEnd(isIncludeEnd);
        requestBuilder.setIsSalting(isSalting);

        try {

//            Table table = connection.getTable(TableName.valueOf(tableName));
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            Map<byte[], List<DataQueryProtos.DataQueryResponse.Row>> result = table.coprocessorService(DataQueryProtos.QueryDataService.class, null, null, counter -> {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<DataQueryProtos.DataQueryResponse> call = new BlockingRpcCallback<>();
                counter.queryByStartRowAndEndRow(controller, requestBuilder.build(), call);
                DataQueryProtos.DataQueryResponse response = call.get();

                if (controller.failedOnException()) {
                    throw controller.getFailedOn();
                }

                return response.getRowListList();
            });

            List<DataQueryProtos.DataQueryResponse.Row> list = new LinkedList<>();
            for (Map.Entry<byte[], List<DataQueryProtos.DataQueryResponse.Row>> entry : result.entrySet()) {
                if (null != entry.getKey()) {
                    list.addAll(entry.getValue());
                }
            }
            return list;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        String tableName = "test_lonLat_region";
        String startRow = "wm4tr1nj";
        String stopRow = "wm5tdt0m";
        List<DataQueryProtos.DataQueryResponse.Row> rows =
                queryByStartRowAndStopRow(tableName, startRow, stopRow, false, true);
        if (null != rows) {
            System.out.println(rows.size());
            for (DataQueryProtos.DataQueryResponse.Row row : rows) {
                List<DataQueryProtos.DataQueryResponse.Cell> cellListList = row.getCellListList();
                for (DataQueryProtos.DataQueryResponse.Cell cell : cellListList) {
                    System.out.println(row.getRowKey().toStringUtf8() + " \t " +
                            "column=" + cell.getFamily().toStringUtf8() +
                            ":" + cell.getQualifier().toStringUtf8() + ", " +
                            "timestamp=" + cell.getTimestamp() + ", " +
                            "value=" + cell.getValue().toStringUtf8());
                }
            }
        }
    }
}
