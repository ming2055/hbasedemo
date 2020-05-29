package com.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.IOException;

//spark-submit --class com.hbase.util.SparkRDD2HBase BigData-1.0-SNAPSHOT.jar
public class SparkRDD2HBase {
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
        JobConf jobConf = new JobConf(configuration, SparkRDD2HBase.class);
        // 设置表名
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbase_table);
        Job job = null;
        try {
            job = Job.getInstance(jobConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Result.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        SparkConf conf = new SparkConf().setAppName("SparkRDD2HBase");
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


        df.javaRDD().mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                String rowkey = row.getString(0);
                String custid = row.getString(1);
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(custid));
                return new Tuple2<>(new ImmutableBytesWritable(), put);
            }
        }).saveAsNewAPIHadoopDataset(job.getConfiguration());


        sc.stop();


    }
}
