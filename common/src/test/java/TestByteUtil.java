import org.vpzlin.javago.utils.ByteUtil;

public class TestByteUtil {
    public static void Test01(){
        int obj = 13;

        byte[] bytes = ByteUtil.toByteArray(obj);
        int result = (int)ByteUtil.toObject(bytes);

        System.out.println(result);
    }

    public static void main(String[] args){
        Test01();
    }
}
