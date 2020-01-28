package org.vpzlin.javago.utils.elastic.elasticsearch;

import org.elasticsearch.client.RestHighLevelClient;
import org.vpzlin.javago.utils.Result;

import java.util.Arrays;

public class DocumentUtil {
    /**
     * default parameters
     */
    // the connection timeout minutes of ElasticSearch master node
    private int timeoutMinutesMasterNode = 1;
    // the connection timeout minutes of ElasticSearch all nodes
    private int timeoutMinutesAllNodes  = 2;

    // the ElasticSearch connection client
    private RestHighLevelClient client;

    /**
     * transform array to String without bracket
     * @param array array of String
     * @return String
     */
    private static String transformArrayToStringWithoutBracket(String[] array){
        if(array == null){
            return null;
        }

        return Arrays.toString(array).replace("[", "").replace("]", "").replace(" ", "");
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param client
     * @throws Exception
     */
    public DocumentUtil(RestHighLevelClient client) throws Exception{
        if(client == null){
            throw new Exception("Failed to init class [DocumentUtil], the input parameter [RestHighLevelClient client] can't be null.");
        }

        this.client = client;
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @param connectProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @param connectTimeout connection timeout seconds, the default value is [1800]
     * @param socketTimeout socket timeout seconds, the default value is [7200]
     * @throws Exception
     */
    public DocumentUtil(String[] serversIP, String serverPort, String connectProtocol, int connectTimeout, int socketTimeout) throws Exception{
        Result result = ClientUtil.getClient(serversIP, serverPort, connectProtocol, connectTimeout, socketTimeout);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil], more info = [%s]", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @throws Exception
     */
    public DocumentUtil(String[] serversIP, String serverPort) throws Exception{
        Result result = ClientUtil.getClient(serversIP, serverPort);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil], more info = [%s]", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @throws Exception
     */
    public DocumentUtil(String[] serversIP) throws Exception{
        Result result = ClientUtil.getClient(serversIP);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil], more info = [%s]", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @param connectProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @param connectTimeout connection timeout seconds, the default value is [1800]
     * @param socketTimeout socket timeout seconds, the default value is [7200]
     * @throws Exception
     */
    public DocumentUtil(String serverIP, String serverPort, String connectProtocol, int connectTimeout, int socketTimeout) throws Exception{
        Result result = ClientUtil.getClient(serverIP, serverPort, connectProtocol, connectTimeout, socketTimeout);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil], more info = [%s]", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @throws Exception
     */
    public DocumentUtil(String serverIP, String serverPort) throws Exception{
        Result result = ClientUtil.getClient(serverIP, serverPort);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil], more info = [%s]", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @throws Exception
     */
    public DocumentUtil(String serverIP) throws Exception{
        Result result = ClientUtil.getClient(serverIP);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil], more info = [%s]", result.getMessage()));
        }
    }

    /**
     * set connection client
     * @param client
     */
    public void setClient(RestHighLevelClient client){
        this.client = client;
    }

    /**
     * get connection client
     * @return connection client
     */
    public RestHighLevelClient getClient(){
        return this.client;
    }

    /**
     * close ElasticSearch connection client
     * @param client the ElasticSearch connection client
     * @return
     */
    public Result closeClient(RestHighLevelClient client){
        return ClientUtil.closeClient(client);
    }

}
