package org.vpzlin.javago.utils.elastic.elasticsearch;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.vpzlin.javago.utils.Result;

import java.sql.*;
import java.util.Properties;

public class SqlUtil {
    // ElasticSearch SQL connection
    private static Connection connection;

    /**
     * init class with JDBC connection to ElasticSearch server
     * @param hostIP ElasticSearch JDBC server ip
     * @param hostPort ElasticSearch JDBC server port
     * @param isHttpsProtocol if ElasticSearch JDBC server opened HTTPS protocol, set this to [true], otherwise set this to [false] which means [http] protocol
     * @throws Exception
     */
    public SqlUtil(String hostIP, String hostPort, boolean isHttpsProtocol) throws Exception{
        try {
            connection = getConnection(hostIP, hostPort, null, null, isHttpsProtocol);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to init class [SqlUtil]. %s", e.getMessage()));
        }
    }

    /**
     * init class with JDBC connection to ElasticSearch server
     * @param hostIP ElasticSearch JDBC server ip
     * @param hostPort ElasticSearch JDBC server port
     * @throws Exception
     */
    public SqlUtil(String hostIP, String hostPort) throws Exception{
        try {
            connection = getConnection(hostIP, hostPort, null, null, false);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to init class [SqlUtil]. %s", e.getMessage()));
        }
    }

    /**
     *
     * @param hostIP ElasticSearch JDBC server ip
     * @param hostPort ElasticSearch JDBC server port
     * @param username ElasticSearch JDBC server username
     * @param password ElasticSearch JDBC server password
     * @param isHttpsProtocol if ElasticSearch JDBC server opened HTTPS protocol, set this to [true], otherwise set this to [false] which means [http] protocol
     * @throws Exception
     */
    public SqlUtil(String hostIP, String hostPort, String username, String password, boolean isHttpsProtocol) throws Exception{
        try {
            connection = getConnection(hostIP, hostPort, username, password, isHttpsProtocol);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to init class [SqlUtil]. %s", e.getMessage()));
        }
    }

    /**
     *
     * @param hostIP ElasticSearch JDBC server ip
     * @param hostPort ElasticSearch JDBC server port
     * @param username ElasticSearch JDBC server username
     * @param password ElasticSearch JDBC server password
     * @throws Exception
     */
    public SqlUtil(String hostIP, String hostPort, String username, String password) throws Exception{
        try {
            connection = getConnection(hostIP, hostPort, username, password, false);
        } catch (Exception e) {
            throw new Exception(String.format("Failed to init class [SqlUtil]. %s", e.getMessage()));
        }
    }

    /**
     * get JDBC connection
     * @param hostIP ElasticSearch JDBC server ip
     * @param hostPort ElasticSearch JDBC server port
     * @param username ElasticSearch JDBC server username
     * @param password ElasticSearch JDBC server password
     * @param isHttpsProtocol if ElasticSearch JDBC server opened HTTPS protocol, set this to [true], otherwise set this to [false] which means [http] protocol
     * @return
     * @throws Exception
     */
    private Connection getConnection(String hostIP, String hostPort, String username, String password, boolean isHttpsProtocol) throws Exception{
        String protocol = "http";
        if(isHttpsProtocol == true){
            protocol = "https";
        }

        String jdbcString = "jdbc:es://"+ protocol + "://" + hostIP + ":" + hostPort;

        Properties connectionProperties = new Properties();
        if(username != null){
            connectionProperties.put("user", username);
            connectionProperties.put("password", password);
        }

        try {
            Connection connection = DriverManager.getConnection(jdbcString, connectionProperties);
            return connection;
        }
        catch (Exception e){
            e.printStackTrace();
            throw new Exception(String.format("Failed to get ElasticSearch JDBC connection, more info = [%s].", e.getMessage()));
        }
    }

    /**
     * query SQL
     * @param sql SQL string
     * @return the type of Result.data is [Map<String, Object>]
     */
    public Result querySQL(String sql){
        if(sql == null || sql.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to , the SQL string can't be null or empty."));
        }

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            // get counting number of columns
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // fetch data
            JSONArray jsonArray = new JSONArray();
            while (resultSet.next()){
                JSONObject jsonObject = new JSONObject();
                for(int i = 1; i <= columnCount; i++){
                    String fieldName = metaData.getColumnName(i);
                    String fieldValue = resultSet.getString(i);
                    if(fieldValue != null && fieldValue.trim().length() > 0){
                        jsonObject.put(fieldName, fieldValue);
                    }
                }
                jsonArray.add(jsonObject);
            }
            return Result.getResult(true, jsonArray, String.format("Finished querying SQL [%s].", sql));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to query SQL [%s], more info = [%s].", sql, e.getMessage()));
        }
    }
}
