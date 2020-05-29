package com.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;

//spark-submit --class com.hbase.util.SparkBulkLoad2HBase BigData-1.0-SNAPSHOT.jar
public class SparkBulkLoad2HBase {
    public static String zookeeperQuorum = "172.16.13.185:2181";
    public static String dataSourcePath = "file:///D:/data/news_profile_data.txt";
    public static String hdfsRootPath = "hdfs://cmiotBigdata";
    public static String hFilePath = "hdfs://cmiotBigdata/cmiotBigdata/test/";
    public static String tableName = "test_hbase";
    public static String familyName = "cf";
    public static String qualifierName = "custid";

    public static Configuration hbaseConf;
    public static Connection hbaseConn;

    //init config
    static {
        //init hbase config
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.property.clienport", "2181");
        hbaseConf.set("hbase.zookeeper.quorum", "datanode45,datanode61,datanode46");
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        //init zookeeper 2181
//		 configuration.set("hbase.zookeeper.property.clienport", "2181");
//		 //init zookeeper host master
//		 configuration.set("hbase.zookeeper.quorum", "master");
//		 //init hbase master config
//		 configuration.set("hbase.master", "master:60000");
        try {
            hbaseConn = ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", hdfsRootPath);
        FileSystem fileSystem = FileSystem.get(hadoopConf);

        // 如果存放 HFile文件的路径已经存在，就删除掉
        if(fileSystem.exists(new Path(hFilePath))) {
            fileSystem.delete(new Path(hFilePath), true);
        }

        SparkConf conf = new SparkConf().setAppName("SparkBulkLoad2HBase");
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

        // 1. 清洗需要存放到 HFile 中的数据，rowKey 一定要排序，否则会报错：
        // java.io.IOException: Added a key not lexically larger than previous.
        JavaPairRDD javaPairRDD = df.javaRDD()
//                .flatMapToPair(new PairFlatMapFunction<Row, String, String>() {
//
//            @Override
//            public Iterable<Tuple2<String, String>> call(Row row) throws Exception {
//                String rowkey = row.getString(0);
//                String custid = row.getString(1);
//                String provinceid = row.getString(2);
//                ArrayList<Tuple2<String, String>> list = new ArrayList<>();
//                list.add(new Tuple2<>(rowkey, custid));
//                list.add(new Tuple2<>(rowkey, provinceid));
//                return list;
//            }
//        })
                .mapToPair(new PairFunction<Row, String, String>() {

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String rowkey = row.getString(0);
                String custid = row.getString(1);
                return new Tuple2<>(rowkey, custid);
            }
        })
                .sortByKey().mapToPair(new PairFunction<Tuple2<String,String>, ImmutableBytesWritable, KeyValue>() {

            @Override
            public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String rowkey = stringStringTuple2._1();
                String custid = stringStringTuple2._2();
                KeyValue kv = new KeyValue(Bytes.toBytes(rowkey),
                        Bytes.toBytes(familyName),
                        Bytes.toBytes(qualifierName),
                        Bytes.toBytes(custid)
                        );

                return new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)),kv);
            }
        });

        // 2. Save Hfiles on HDFS
        Admin admin = hbaseConn.getAdmin();
        Table table = hbaseConn.getTable(TableName.valueOf(tableName));
        Job job = Job.getInstance(hbaseConf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2.configureIncrementalLoadMap(job, table);

        //方法1：saveAsNewAPIHadoopDataset
        job.getConfiguration().set("mapred.output.dir", hFilePath);
        javaPairRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());

        //方法2：saveAsNewAPIHadoopFile
//        javaPairRDD.saveAsNewAPIHadoopFile(
//                hFilePath,
//                ImmutableBytesWritable.class,
//                KeyValue.class,
//                HFileOutputFormat2.class,
//                hbaseConf
//        );


        //  3. Bulk load Hfiles to Hbase
        LoadIncrementalHFiles bulkLoader = null;
        try {
            bulkLoader = new LoadIncrementalHFiles(hbaseConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        RegionLocator regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName));
        bulkLoader.doBulkLoad(new Path(hFilePath), admin, table, regionLocator);

        hbaseConn.close();

        sc.stop();
        //需要在sc.stop()之后否则会报错 Filesystem closed
        fileSystem.close();

    }


}
