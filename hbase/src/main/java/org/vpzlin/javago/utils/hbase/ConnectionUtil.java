package org.vpzlin.javago.utils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.vpzlin.javago.utils.Result;

import java.io.IOException;

public class ConnectionUtil {
    /**
     * get HBase connection
     * @param zookeeperServers zookeeper server ip or servers' names, separated by commas
     * @param zookeeperPort zookeeper server port
     * @param connectionPoolSize connection pool size, default value is [1]
     * @return the type of Result.data is [org.apache.hadoop.hbase.client.Connection]
     */
    public static Result getHBaseConnection(String zookeeperServers, String zookeeperPort, int connectionPoolSize){
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperServers);
        config.set("hbase.zookepper.property.clientPort", zookeeperPort);
        config.set("hbase.client.ipc.pool.size", String.valueOf(connectionPoolSize));
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            return Result.getResult(true, connection, String.format("Got connection to HBase server [%s] through port [%s].", zookeeperServers, zookeeperPort));
        } catch (IOException e) {
            return Result.getResult(false, null, String.format("Failed to get connection to HBase server [%s] through port [%s].", zookeeperServers, zookeeperPort));
        }
    }

    /**
     * get HBase connection
     * @param zookeeperServers zookeeper server ip or servers' names, separated by commas
     * @param zookeeperPort zookeeper server port
     * @return the type of Result.data is [org.apache.hadoop.hbase.client.Connection]
     */
    public static Result getHBaseConnection(String zookeeperServers, String zookeeperPort){
        return getHBaseConnection(zookeeperServers, zookeeperPort, 1);
    }
}
