package com.netease.sloth.parser;

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;

import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * New datastream schema parser
 *
 * !!!warning: the class should not be modified
 */
public class NewDsParser extends SlothKafkaParserBase {

	private String[] columnNames;

	public NewDsParser(RichTableSchema tableSchema, TableProperties properties) {
		super(tableSchema, properties);
		columnNames = tableSchema.getColumnNames();
	}


	@Override
	public void eval(byte[] messageKey, byte[] message, String topic, int partition, long offset) {

		String msg = new String(message, StandardCharsets.UTF_8);

		JSONObject jsonObject = new JSONObject(msg);

		String[] fields = new String[columnNames.length];
		for (int i = 0; i < columnNames.length; i++) {
			fields[i] = jsonObject.getString(columnNames[i].toLowerCase());
		}

		collect(Row.of(fields));
	}
}

