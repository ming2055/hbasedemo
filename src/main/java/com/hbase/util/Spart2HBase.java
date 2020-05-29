package com.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.Iterator;

//spark-submit --class com.hbase.util.Spart2HBase BigData-1.0-SNAPSHOT.jar
public class Spart2HBase {
    private static Logger logger = Logger.getLogger(HBaseUtil.class);

    //LoggerFactory.getLogger(HBaseUtil.class);
    //configuration
    public static Configuration configuration;
    public static Connection connection;

    public static String hbase_table = "test_hbase";
    public static String family = "cf";
    public static String column = "custid";

    //init config
    static {
        //init hbase config
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clienport", "2181");
        configuration.set("hbase.zookeeper.quorum", "datanode45,datanode61,datanode46");
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

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spart2HBase");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new HiveContext(sc);
        String str_sql = "select msisdn,\n" +
                "       custid,\n" +
                "       provinceid,\n" +
                "       LOGICREGIONID,\n" +
                "       '01' as business_system,\n" +
                "       card_type,\n" +
                "       NONDIRECT_CALL_FLAG,\n" +
                "       white_list_flag\n" +
                "from BD_DM_SUBS_SCENE_RISK_ED_DAY\n" +
                "where day_id = '20180901'\n" +
                "limit 10000";
        DataFrame df = sqlContext.sql(str_sql);
        df.show();


        df.javaRDD().foreachPartition(rows -> {
            //不需要在里面创建connection对象
//            configuration = HBaseConfiguration.create();
//            configuration.set("hbase.zookeeper.property.clienport", "2181");
//            configuration.set("hbase.zookeeper.quorum", "datanode45,datanode61,datanode46");

            //不设定也没问题
//            configuration.set("hbase.master", "master:60000");

            try {
//                connection = ConnectionFactory.createConnection(configuration);
                Table table = null;
                table = connection.getTable(TableName.valueOf(hbase_table));

                while (rows.hasNext()) {
                    Row row = rows.next();
                    String rowkey = row.getString(0);
                    String value = row.getString(1);
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                    table.put(put);
                }
                table.close();
//                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


        sc.stop();


    }


}
