package com.netease.sloth.parser;

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Base kafka source parser
 * user has to implement eval func
 * the params of eval func are:
 * 1. messageKey - current message key of kafka
 * 2. `message` - current kafka message
 * 3. topic - the kafka topic of current message
 * 4. `partition` - the partition of current message
 * 5. `offset` - the partition offset of current message
 *
 * !!!warning: the class should not be modified
 */
public abstract class SlothKafkaParserBase extends TableFunction<Row> implements Serializable {

	protected static final Logger LOG = LoggerFactory.getLogger(SlothKafkaParserBase.class);

	protected RichTableSchema tableSchema;

	protected TableProperties properties;

	public SlothKafkaParserBase(RichTableSchema tableSchema, TableProperties properties) {
		this.tableSchema = tableSchema;
		this.properties = properties;
	}

	public abstract void eval(byte[] messageKey, byte[] message, String topic, int partition, long offset);

	public DataType getResultType(Object[] arguments, Class[] argTypes) {
		return tableSchema.getResultRowType();
	}
}
