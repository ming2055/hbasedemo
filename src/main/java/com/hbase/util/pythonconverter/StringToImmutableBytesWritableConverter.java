package com.hbase.util.pythonconverter;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.python.Converter;

// com.hbase.util.pythonconverter.StringToImmutableBytesWritableConverter
public class StringToImmutableBytesWritableConverter implements Converter<Object, ImmutableBytesWritable> {
    @Override
    public ImmutableBytesWritable convert(Object obj) {
        String rowkey = (String) obj;
        return new ImmutableBytesWritable(Bytes.toBytes(rowkey));
    }
}
