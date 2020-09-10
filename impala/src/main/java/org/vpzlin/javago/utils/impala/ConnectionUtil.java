package org.vpzlin.javago.utils.impala;

import org.vpzlin.javago.utils.Result;

import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectionUtil {
    /**
     * get impala connection
     * @param hostIP host ip
     * @param hostPort host port
     * @param databaseName impala database name
     * @return the type of Result.data is [java.sql.Connection]
     */
    public static Result getImpalaJdbcConnection(String hostIP, String hostPort, String databaseName){
        String jdbcDriver = "com.cloudera.impala.jdbc41.Driver";
        String jdbcUrl = "jdbc:impala://" + hostIP + ":" + hostPort + "/" + databaseName;

        try {
            Class.forName(jdbcDriver);
            Connection connection = DriverManager.getConnection(jdbcUrl);
            String info = String.format("Got impala jdbc connection. Host IP = [%s]. Host Port = [%s].", hostIP, hostPort);
            if(databaseName != null && databaseName.trim().length() > 0){
                info += String.format(" Database = [%s].", databaseName);
            }
            return Result.getResult(true, connection, info);
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to get impala jdbc connection, more info = [%s].", e.getMessage()));
        }
    }

    /**
     * get impala connection
     * @param hostIP host ip
     * @param hostPort host port
     * @return the type of Result.data is [java.sql.Connection]
     */
    public static Result getImpalaJdbcConnection(String hostIP, String hostPort){
        return getImpalaJdbcConnection(hostIP, hostPort, null);
    }
}
