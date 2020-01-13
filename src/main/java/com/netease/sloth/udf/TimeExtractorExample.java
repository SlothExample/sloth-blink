package com.netease.sloth.udf;

import org.apache.flink.table.api.functions.ScalarFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * udf example
 */
public class TimeExtractorExample extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(TimeExtractorExample.class);

    public String eval(byte[] message) {

        // 1526650195000,id,name
        try {
            String content = new String(message, "UTF-8");
            return content.split(",")[0];
        } catch (Exception e) {
            LOG.warn("Dirty data: " + new String(message), e);
            return null;
        }
    }
}
