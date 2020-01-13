package com.netease.sloth.parser;

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;

import org.apache.commons.lang.ArrayUtils;

import java.nio.charset.StandardCharsets;

/**
 * Delimiter parser only parse message with delimiter specified by user
 *
 * !!!warning: the class should not be modified
 */
public class DelimiterParser extends SlothKafkaParserBase {

	private String delimiter;

	public DelimiterParser(RichTableSchema tableSchema, TableProperties properties) {
		super(tableSchema, properties);
		this.delimiter = properties.getString(ParserConstants.DELIMITER, ParserConstants.DEFAULT_DELIMITER);
	}

	public void eval(byte[] messageKey, byte[] message, String topic, int partition, long offset) {

		String msg = new String(message, StandardCharsets.UTF_8);

		String[] fields = msg.split(delimiter);

		if (ArrayUtils.isEmpty(fields) || fields.length > tableSchema.getColumnNames().length) {
			LOG.error("Illegal input fields length: " + msg
				+ ", the length should be: " + tableSchema.getColumnNames().length
				+ "where is the same with that of source table columns.");
		} else {
			collect(Row.of(fields));
		}
	}
}
