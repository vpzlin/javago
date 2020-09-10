package org.vpzlin.javago.utils.impala;

import org.vpzlin.javago.utils.Result;

import java.sql.Connection;

public class ImpalaUtil {
    private Connection connection;

    /**
     * set impala jdbc connection
     * @param hostIP host ip
     * @param hostPort host port
     * @param databaseName impala database name
     * @return
     */
    public Result setJdbcConnection(String hostIP, String hostPort, String databaseName){
        Result connectionResult = ConnectionUtil.getImpalaJdbcConnection(hostIP, hostPort, databaseName);
        if(connectionResult.isSuccess() == true){
            connection = (Connection)connectionResult.getData();
            String info = String.format("Set impala jdbc connection. Host IP = [%s]. Host Port = [%s].", hostIP, hostPort);
            if(databaseName != null && databaseName.trim().length() > 0){
                info += String.format(" Database = [%s].", databaseName);
            }
            return Result.getResult(true, null, info);
        }
        else {
            return Result.getResult(false, null, String.format("Failed to set impala jdbc connection. %s", connectionResult.getMessage()));
        }
    }

    /**
     * set impala jdbc connection
     * @param hostIP host ip
     * @param hostPort host port
     * @return
     */
    public Result setJdbcConnection(String hostIP, String hostPort){
        return setJdbcConnection(hostIP, hostPort, null);
    }
}
