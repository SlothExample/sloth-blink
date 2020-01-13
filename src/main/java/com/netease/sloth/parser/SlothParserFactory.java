package com.netease.sloth.parser;

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.util.TableProperties;

import org.apache.commons.lang.StringUtils;

/**
 * Parser Factory to produce a parserP
 *
 * !!!warning: the class should not be modified
 */
public class SlothParserFactory {

	public static SlothKafkaParserBase getParser(RichTableSchema tableSchema, TableProperties properties)
		throws Exception {

		String parser = properties.getString(ParserConstants.PARSER, null);

		if (StringUtils.isBlank(parser)) {
			throw new RuntimeException("`parser` should not be blank.");
		}

		switch (parser) {
			case ParserConstants.DELIMITER:
				return new DelimiterParser(tableSchema, properties);
			case ParserConstants.OLD_DS:
				return new OldDsParser(tableSchema, properties);
			case ParserConstants.NEW_DS:
				return new NewDsParser(tableSchema, properties);
			default:
				return (SlothKafkaParserBase) Thread.currentThread().getContextClassLoader().loadClass(parser)
					.getConstructor(new Class[]{RichTableSchema.class, TableProperties.class})
					.newInstance(tableSchema, properties);
		}

	}
}
