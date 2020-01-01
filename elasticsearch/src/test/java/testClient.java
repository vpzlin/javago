import org.vpzlin.javago.utils.elastic.elasticsearch.ClientUtil;

public class testClient {
    public static void Test01(){
        String serverIp = "192.168.108.111";
        String[] serversIp = {"192.168.108.111", "192.168.108.111"};
        String serverPort = "9200";
        String connectProtocol = "http";

//        System.out.println(ClientUtil.getClient(serverIp, serverPort).getMessage());
        System.out.println(ClientUtil.getClient(serversIp, serverPort).getMessage());
    }

    public static void main(String[] args){
        Test01();
    }
}
