package com.netease.sloth.parser;

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.nio.charset.StandardCharsets;

/**
 * Old datastream schema parser
 *
 * !!!warning: the class should not be modified
 */
public class OldDsParser extends SlothKafkaParserBase {

	private transient Gson gson;

	private String[] columnNames;

	public OldDsParser(RichTableSchema tableSchema, TableProperties properties) {
		super(tableSchema, properties);
		columnNames = tableSchema.getColumnNames();
	}


	@Override
	public void eval(byte[] messageKey, byte[] message, String topic, int partition, long offset) {

		String msg = new String(message, StandardCharsets.UTF_8);

		if (gson == null) {
			gson = new Gson();
		}

		OldDsKafkaSchema dsKafkaSchema = gson.fromJson(msg, OldDsKafkaSchema.class);
		if (null == dsKafkaSchema) {
			return;
		}

		String[] fields = new String[columnNames.length];
		for (int i = 0; i < columnNames.length; i++) {
			fields[i] = getField(msg, dsKafkaSchema, columnNames[i].toLowerCase());
		}

		collect(Row.of(fields));
	}

	private String getField(String raw, OldDsKafkaSchema dsKafkaSchema, String fieldName) {
		switch (fieldName) {
			case "raw":
				return raw;
			case "nanos":
				return dsKafkaSchema.nanos;
			case "timestamp":
				return dsKafkaSchema.timestamp;
			case "body":
				return dsKafkaSchema.body;
			case "host":
				return dsKafkaSchema.host;
			case "priority":
				return dsKafkaSchema.priority;
			case "fields_hostname":
				return dsKafkaSchema.fields.hostname;
			case "fields_ds_position":
				return dsKafkaSchema.fields.dsPosition;
			case "fields_ds_time_stamp":
				return dsKafkaSchema.fields.dsTimestamp;
			case "fields_ds_unique_id":
				return dsKafkaSchema.fields.dsUniqueId;
			case "fields_ds_file_inode":
				return dsKafkaSchema.fields.dsFileInode;
			case "fields_ds_target_dir":
				return dsKafkaSchema.fields.dsTargetDir;
			case "fields_ds_file_pattern":
				return dsKafkaSchema.fields.dsFilePattern;
			case "fields_ds_file_fingerprint":
				return dsKafkaSchema.fields.dsFileFingerprint;
			case "fields_ds_agent_id":
				return dsKafkaSchema.fields.dsAgentId;
			case "fields_tag":
				return dsKafkaSchema.fields.tag;
			case "fields_ds_file_name":
				return dsKafkaSchema.fields.dsFileName;
			default:
				throw new RuntimeException("unsupported field of ds kafka source.");
		}
	}

	/**
	 * Ds kafka source schema without "fields" field: http://doc.hz.netease.com/pages/viewpage.action?pageId=59287499
	 */
	private static class OldDsKafkaSchema {
		String nanos;
		String timestamp;
		String body;
		String host;
		String priority;
		Fields fields;

		@Override
		public String toString() {
			return "OldDsKafkaSchema{"
				+ "nanos='" + nanos + '\''
				+ ", timestamp='" + timestamp + '\''
				+ ", body='" + body + '\''
				+ ", host='" + host + '\''
				+ ", priority='" + priority + '\''
				+ ", fields=" + fields
				+ '}';
		}
	}

	/**
	 * Ds kafka source schema, sub-fields
	 */
	private static class Fields {
		@SerializedName(value = "HOSTNAME")
		String hostname;

		@SerializedName(value = "_ds_position")
		String dsPosition;

		@SerializedName(value = "_ds_time_stamp")
		String dsTimestamp;

		@SerializedName(value = "_ds_unique_id")
		String dsUniqueId;

		@SerializedName(value = "_ds_file_inode")
		String dsFileInode;

		@SerializedName(value = "_ds_target_dir")
		String dsTargetDir;

		@SerializedName(value = "_ds_file_pattern")
		String dsFilePattern;

		@SerializedName(value = "_ds_file_fingerprint")
		String dsFileFingerprint;

		@SerializedName(value = "_ds_agent_id")
		String dsAgentId;

		@SerializedName(value = "TAG")
		String tag;

		@SerializedName(value = "_ds_file_name")
		String dsFileName;

		@Override
		public String toString() {
			return "Fields{"
				+ "hostname='" + hostname + '\''
				+ ", dsPosition='" + dsPosition + '\''
				+ ", dsTimestamp='" + dsTimestamp + '\''
				+ ", dsUniqueId='" + dsUniqueId + '\''
				+ ", dsFileInode='" + dsFileInode + '\''
				+ ", dsTargetDir='" + dsTargetDir + '\''
				+ ", dsFilePattern='" + dsFilePattern + '\''
				+ ", dsFileFingerprint='" + dsFileFingerprint + '\''
				+ ", dsAgentId='" + dsAgentId + '\''
				+ ", tag='" + tag + '\''
				+ ", dsFileName='" + dsFileName + '\''
				+ '}';
		}
	}

}
