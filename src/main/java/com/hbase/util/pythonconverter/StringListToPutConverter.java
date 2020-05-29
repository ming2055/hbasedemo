package com.hbase.util.pythonconverter;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.python.Converter;

import java.util.ArrayList;
import java.util.Iterator;

//com.hbase.util.pythonconverter.StringListToPutConverter
@SuppressWarnings("unchecked")
public class StringListToPutConverter implements Converter<Object, Put> {
    @Override
    public Put convert(Object obj) {
        ArrayList<String> list = (ArrayList<String>) obj;
        Iterator<String> iter = list.listIterator();
        String rowkey = iter.next();
//        [rowkey, columnFamily, 'custid', columnValue1, 'clm2', clv2]
        byte[] cf = Bytes.toBytes(iter.next());
        byte[] columnName;
        byte[] columnValue;
        Put put = new Put(Bytes.toBytes(rowkey));
        while (iter.hasNext()) {
            columnName = Bytes.toBytes(iter.next());
            columnValue = Bytes.toBytes(iter.next());
            put.addColumn(cf, columnName, columnValue);
        }
        return put;
    }
}
