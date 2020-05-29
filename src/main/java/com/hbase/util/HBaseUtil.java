package com.hbase.util;
/**
 * HBase Util API V1.0
 *
 * @author root
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import com.common.Util;
import com.geohash.GeoHashUtil;
import com.geohash.RondomLonLat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class HBaseUtil {
    private static Logger logger = Logger.getLogger(HBaseUtil.class);

    //LoggerFactory.getLogger(HBaseUtil.class);
    //configuration
    public static Configuration configuration;
    public static Connection connection;

    //init config
    static {
        //init hbase config
        configuration = HBaseConfiguration.create();
//        configuration.addResource("hbase-site.xml");
        //init zookeeper 2181
//		 configuration.set("hbase.zookeeper.property.clienport", "2181");
//		 //init zookeeper host master
//		 configuration.set("hbase.zookeeper.quorum", "master");
//		 //init hbase master config
//		 configuration.set("hbase.master", "master:60000");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //create table
    public static void createTable(String tabName, String... familys) throws Exception {
        //create admin
        Admin admin = connection.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tabName));
        for (String string : familys) {
            HColumnDescriptor columnCF = new HColumnDescriptor(string);
            table.addFamily(columnCF);
        }
        if (admin.tableExists(TableName.valueOf(tabName))) {
            System.out.println("table already exists");
        } else {
            admin.createTable(table);
            System.out.println("successed!" + tabName);
        }
        //close admin
        admin.close();
    }

    /**
     * 要进行预分区，首先要明确rowkey的取值范围或构成逻辑，以我的rowkey组成为例:两位随机数+时间戳+客户号，两位随机数的范围从00-99，于是我划分了10个region来存储数据,每个region对应的rowkey范围如下：
     * -10,10-20,20-30,30-40,40-50,50-60,60-70,70-80,80-90,90-
     * 在使用HBase API建表的时候，需要产生splitkeys二维数组,这个数组存储的rowkey的边界值
     *
     * @return
     */
    public static byte[][] getSplitKeys() {
//        String[] keys = new String[]{"a", "b", "c"};
        String[] keys = Util.getHexSpiltKeys();
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序,必须排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }

    //create table
    public static void createTableBySplitKeys(String tabName, String... familys) throws Exception {
        //create admin
        Admin admin = connection.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tabName));
        for (String string : familys) {
            HColumnDescriptor columnCF = new HColumnDescriptor(string);
//            columnCF.setTimeToLive(3 * 60 * 60 * 24);
//            columnCF.setMaxVersions(3);
            table.addFamily(columnCF);
        }
        if (admin.tableExists(TableName.valueOf(tabName))) {
            System.out.println("table already exists");
        } else {
            byte[][] splitKeys = getSplitKeys();
            admin.createTable(table, splitKeys);
            System.out.println("successed!" + tabName);
        }
        //close admin
        admin.close();
    }

    //delete table
    public static void deleteTable(String tabName) {
        //create admin
        try {
            Admin admin = connection.getAdmin();
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tabName));
            if (admin.tableExists(TableName.valueOf(tabName))) {
                if (admin.isTableDisabled(TableName.valueOf(tabName))) {
                    admin.deleteTable(TableName.valueOf(tabName));
                    System.out.println("delete table " + tabName + " successed");
                } else {
                    admin.disableTable(TableName.valueOf(tabName));
                    System.out.println("disable table");
                    admin.deleteTable(TableName.valueOf(tabName));
                    System.out.println("delete table " + tabName + " successed");
                }

            } else {
                System.out.println("table not exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //insert data
    //1.table
    //2.put
    //3.add data into put
    //4.table.put(put)

    //one family, one column ,one value
    public static void insertData(String tabName, String rowkey, String family, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("Insert Successed" + rowkey);
    }

    //insertDataByPutList
    public static void insertDataByPutList(String tableName, List<Put> putList) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        logger.info("开始插入数据");
        table.put(putList);
        table.close();
    }

    //create put
    public static Put createPut(String rowkey, String family, String column, String value) throws IOException {
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        return put;
    }


    //one time cf , columns, values
    public static void insertDatas(String tabName, String rowkey, String family, String[] columns, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tabName));
        Put put = new Put(Bytes.toBytes(rowkey));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
        }
        table.put(put);
        table.close();
        System.out.println("Insert Successed!" + rowkey);
    }

    public static void searchByRowFilter(String tableName, String rowkey) {

        long startTime = System.currentTimeMillis();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rowkey)));
            scan.setFilter(rowFilter);
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("rowkey:" + Bytes.toString(result.getRow()) + "   "
                            + Bytes.toString(CellUtil.cloneFamily(cell)) + "    "
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + "    "
                            + Bytes.toString(CellUtil.cloneValue(cell)));

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println((System.currentTimeMillis() - startTime) / 1000 + "s");
    }

    public static void searchByGet(String tableName, String rowkey) {
        long startTime = System.currentTimeMillis();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell)) + "    "
                        + Bytes.toString(CellUtil.cloneFamily(cell)) + "    "
                        + Bytes.toString(CellUtil.cloneQualifier(cell)) + "    "
                        + Bytes.toString(CellUtil.cloneValue(cell))
                );
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println((System.currentTimeMillis() - startTime) / 1000 + "s");
    }

    public static void scanByPrefixFilter(String tableName, String rowPrefix) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(new PrefixFilter(rowPrefix.getBytes()));
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell)) + "   "
                            + Bytes.toString(CellUtil.cloneFamily(cell)) + "    "
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + "    "
                            + Bytes.toString(CellUtil.cloneValue(cell)));

                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void scanByStartEndKey(String tableName, String startKey,String endKey) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(endKey));
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell)) + "   "
                            + Bytes.toString(CellUtil.cloneFamily(cell)) + "    "
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + "    "
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws Exception {

        logger.info("begin");
//        String tableName = "test01:person3";
//        String[] families = new String[]{"cf", "cf1"};
//
//        System.out.println(configuration.get("hbase.zookeeper.quorum"));
//        HBaseUtil.createTable(tableName, families);
//        HBaseUtil.deleteTable(tableName);
//		String family = "cf";
//		String[] columns = new String[] {"name","address"};
//		String[] values = new String[] {"terry","beijing"};
//		HBaseUtil.insertData(tableName, "raw02", "cf", "name", "tom");
//		HBaseUtil.insertDatas("person3","raw03",family,columns,values);
//        searchByRowFilter("bd_hbase_risk_subsinfo_ed_day", "1064705283280");
//        searchByGet("bd_hbase_risk_subsinfo_ed_day", "1064705283280");

        //建表
        String tableName = "test_lonLat_region";
        String family = "cf";
        String column = "name";
        HBaseUtil.createTableBySplitKeys(tableName, family);

        //随机获取100条经纬度，并geohash
//        int num_sample = 100;
//        GeoHashUtil geoHashUtil = null;
//        String[] geohashes = new String[num_sample];
//        RondomLonLat rondomLonLat = new RondomLonLat(105, 108, 29, 30);
//        for (int i = 0; i < num_sample; i++) {
//            String[] lonLat = rondomLonLat.randomLonLat();
//            double lon = Double.parseDouble(lonLat[0]);
//            double lat = Double.parseDouble(lonLat[1]);
//            geoHashUtil = new GeoHashUtil(lat, lon);
//            geohashes[i] = geoHashUtil.getGeoHashBase32();
//        }
//
//        //向HBase插入数据
//        List<Put> putList = new ArrayList<>();
//        Put put = null;
//        for (int i = 0; i < geohashes.length; i++) {
//            String rowKey = new Util().getHBaseSalt() + "-" + geohashes[i];
//            put = createPut(rowKey, family, column, Integer.toString(i));
//            putList.add(put);
//        }
//        insertDataByPutList(tableName, putList);
//        searchByGet(tableName, "a-wmkc1j6x");
    }
}
