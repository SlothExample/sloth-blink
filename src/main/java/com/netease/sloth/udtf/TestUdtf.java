package com.netease.sloth.udtf;

import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.types.Row;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * udtf example
 */
public class TestUdtf extends TableFunction<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(TestUdtf.class);

    public void eval(byte[] message) {

        // 1526650195000,id,name
        try {
            String content = new String(message, "UTF-8");
            String[] fields = content.split(",");

            if (ArrayUtils.isNotEmpty(fields) && fields.length == 3) {
                Row row = new Row(2);
                row.setField(0, Integer.valueOf(fields[1]));
                row.setField(1, fields[1]);

                collect(row);
            }
        } catch (Exception e) {
            LOG.warn("Dirty data: " + new String(message), e);
        }
    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return DataTypes.createRowType(DataTypes.INT, DataTypes.STRING);
    }
}