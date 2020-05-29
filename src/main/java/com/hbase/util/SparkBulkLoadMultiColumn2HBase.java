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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//spark-submit --class com.hbase.util.SparkBulkLoadMultiColumn2HBase --conf spark.executor.memory=15G --conf spark.default.parallelism=200 --conf "spark.executorEnv.JAVA_HOME=/usr/java/jdk1.8.0_181-amd64" BigData-1.0-SNAPSHOT.jar
public class SparkBulkLoadMultiColumn2HBase {

    public static String hdfsRootPath = "hdfs://cmiotBigdata";
    public static String hFilePath = "hdfs://cmiotBigdata/cmiotBigdata/test/";
    //HBase shell 先建表：create 'bd_hbase_risk_subsinfo_ed_day','risk'
    public static String tableName = "bd_hbase_risk_subsinfo_ed_day";
    public static String familyName = "risk";

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
        //spark 访问Hive
        SparkConf conf = new SparkConf().setAppName("SparkBulkLoad2HBase");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new HiveContext(sc);
        String str_sql = "select msisdn               \n" +
                "       ,subs_id             \n" +
                "       ,province_id         \n" +
                "       ,province_name       \n" +
                "       ,region_id           \n" +
                "       ,region_name         \n" +
                "       ,cust_id             \n" +
                "       ,cust_name           \n" +
                "       ,binding_flag        \n" +
                "       ,white_list_flag     \n" +
                "       ,nodirect_open_flag  \n" +
                "       ,business_system     \n" +
                "from bd_hbase_risk_subsinfo_ed_day\n" +
                "where day_id = '20180901'" ;
        System.out.println("============执行查询sql==========");
        System.out.println(str_sql);
        DataFrame df = sqlContext.sql(str_sql).na().fill("");
        df.show();


        // 1. 清洗需要存放到 HFile 中的数据，rowKey 一定要排序，否则会报错：
        // java.io.IOException: Added a key not lexically larger than previous.
        JavaPairRDD javaPairRDD = df.javaRDD().flatMapToPair(new PairFlatMapFunction<Row, ImmutableBytesWritable, KeyValue>() {
                    @Override
                    public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Row row) throws Exception {
                        List<Tuple2<ImmutableBytesWritable, KeyValue>> keyValueList = new ArrayList<>();

//                        String rowkey = row.getString(0).hashCode() + "!" + row.getString(0);
                        String rowkey = row.getString(0);
                        ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes(rowkey));

                        String subs_id = row.getString(1);
                        String province_id = row.getString(2);
                        String province_name = row.getString(3);
                        String region_id = row.getString(4);
                        String region_name = row.getString(5);
                        String cust_id = row.getString(6);
                        String cust_name = row.getString(7);
                        String binding_flag = row.getString(8);
                        String white_list_flag = row.getString(9);
                        String nodirect_open_flag = row.getString(10);
                        String business_system = row.getString(11);

                        //HBase排序规则
                        //先rowkey升序排序，
                        //rowkey相同则column key（column family和qualifier）升序排序
                        //rowkey、column key相同则timestamp降序排序
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("binding_flag"), Bytes.toBytes(binding_flag))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("business_system"), Bytes.toBytes(business_system))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("cust_id"), Bytes.toBytes(cust_id))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("cust_name"), Bytes.toBytes(cust_name))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("nodirect_open_flag"), Bytes.toBytes(nodirect_open_flag))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("province_id"), Bytes.toBytes(province_id))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("province_name"), Bytes.toBytes(province_name))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("region_id"), Bytes.toBytes(region_id))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("region_name"), Bytes.toBytes(region_name))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("subs_id"), Bytes.toBytes(subs_id))));
                        keyValueList.add(new Tuple2<>(writable, new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes(familyName), Bytes.toBytes("white_list_flag"), Bytes.toBytes(white_list_flag))));

                        return keyValueList;
                    }
                }).sortByKey();

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
//                List.class,
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
