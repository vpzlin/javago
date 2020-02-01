package org.vpzlin.javago.utils.elastic.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.vpzlin.javago.utils.Result;

import java.util.Arrays;

public class ClientUtil {
    // the default servers' port to connect
    private static String serverPort = "9200";
    // the default timeout seconds of connection
    private static int connectTimeout = 60 * 30;
    // the default timeout seconds of socket
    private static int socketTimeout = 60 * 60 * 2;


    /**
     * get connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @param isHttpsProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @param connectTimeout connection timeout seconds, the default value is [1800]
     * @param socketTimeout socket timeout seconds, the default value is [7200]
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String[] serversIP, String serverPort, boolean isHttpsProtocol, int connectTimeout, int socketTimeout){
        String connectProtocol = "http";
        if(isHttpsProtocol == true){
            connectProtocol = "https";
        }

        try {
            HttpHost[] httpHosts = new HttpHost[serversIP.length];
            for (int i = 0; i < serversIP.length; i++){
                httpHosts[i] = new HttpHost(serversIP[i], Integer.parseInt(serverPort), connectProtocol);
            }

            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(httpHosts)
                            .setRequestConfigCallback(
                                    new RestClientBuilder.RequestConfigCallback() {
                                        @Override
                                        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
                                            return builder.setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout);
                                        }
                                    }
                            )
            );
            return Result.getResult(true, client, String.format("Connected to ElasticSearch server [%s], the port is [%s], the protocol is [%s].", Arrays.toString(serversIP).replace("[", "").replace("]", ""), serverPort, connectProtocol));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to connect to ElasticSearch server [%s], the port is [%s], the protocol is [%s], more info = [%s].", Arrays.toString(serversIP).replace("[", "").replace("]", ""), serverPort, connectProtocol, e.getMessage()));
        }
    }

    /**
     * get connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @param isHttpsProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String[] serversIP, String serverPort, boolean isHttpsProtocol){
        return getClient(serversIP, serverPort, isHttpsProtocol, connectTimeout, socketTimeout);
    }

    /**
     * get connection to ElasticSearch server with [http] protocol
     * @param serversIP the servers' IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String[] serversIP, String serverPort){
        return getClient(serversIP, serverPort, false, connectTimeout, socketTimeout);
    }

    /**
     * get connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @param isHttpsProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String[] serversIP, boolean isHttpsProtocol){
        return getClient(serversIP, serverPort, isHttpsProtocol, connectTimeout, socketTimeout);
    }

    /**
     * get connection to ElasticSearch server with [http] protocol
     * @param serversIP the servers' IP to connect
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String[] serversIP){
        return getClient(serversIP, serverPort, false, connectTimeout, socketTimeout);
    }

    /**
     * get connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @param isHttpsProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @param connectTimeout connection timeout seconds, the default value is [1800]
     * @param socketTimeout socket timeout seconds, the default value is [7200]
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String serverIP, String serverPort, boolean isHttpsProtocol, int connectTimeout, int socketTimeout){
        String[] hostsIp = {serverIP};
        return getClient(hostsIp, serverPort, isHttpsProtocol, connectTimeout, socketTimeout);
    }

    /**
     * get connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @param isHttpsProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String serverIP, String serverPort, boolean isHttpsProtocol){
        return getClient(serverIP, serverPort, isHttpsProtocol, connectTimeout, socketTimeout);
    }

    /**
     * get connection to ElasticSearch server with [http] protocol
     * @param serverIP the server's IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String serverIP, String serverPort){
        return getClient(serverIP, serverPort, false, connectTimeout, socketTimeout);
    }

    /**
     * get connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @param isHttpsProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String serverIP, boolean isHttpsProtocol){
        return getClient(serverIP, serverPort, isHttpsProtocol, connectTimeout, socketTimeout);
    }

    /**
     * get connection to ElasticSearch server with [http] protocol
     * @param serverIP the server's IP to connect
     * @return the type of Result.data is [RestHighLevelClient]
     */
    public static Result getClient(String serverIP){
        return getClient(serverIP, serverPort, false, connectTimeout, socketTimeout);
    }

    /**
     * close ElasticSearch connection client
     * @param client the ElasticSearch connection client
     * @return
     */
    public static Result closeClient(RestHighLevelClient client){
        try{
            client.close();
            return Result.getResult(true, null, "Closed ElasticSearch client.");
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to close ElasticSearch client, more info = [%s].", e.getMessage()));
        }
    }
}
