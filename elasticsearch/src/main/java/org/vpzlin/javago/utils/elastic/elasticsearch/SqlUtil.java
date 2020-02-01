package org.vpzlin.javago.utils.elastic.elasticsearch;

import java.sql.Connection;

public class SqlUtil {
    // ElasticSearch SQL connection
    private static Connection connection;

    /**
     * default parameters
     */
    // max number of records to return
    private int maxRecordsNumber = 10000;


    // get max number of records to return
    public int getMaxRecordsDefault() {
        return maxRecordsNumber;
    }

    // set max number of records to return
    public void setMaxRecordsDefault(int maxRecordsDefault) {
        this.maxRecordsNumber = maxRecordsDefault;
    }
}
