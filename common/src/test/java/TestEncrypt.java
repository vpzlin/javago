import org.vpzlin.javago.util.EncryptUtil;

public class TestEncrypt {
    public static void test01(){
        String text = "vpzlin";

        System.out.println("md5 result asc2 = " + EncryptUtil.md5EncodeAscII(text));
        System.out.println("md5 result gbk  = " + EncryptUtil.md5EncodeGBK(text));
        System.out.println("md5 result utf8 = " + EncryptUtil.md5EncodeUTF8(text));

        System.out.println("sha256 result asc2 = " + EncryptUtil.sha256EncodeAscII(text));
        System.out.println("sha256 result gbk  = " + EncryptUtil.sha256EncodeGBK(text));
        System.out.println("sha256 result utf8 = " + EncryptUtil.sha256EncodeUTF8(text));
    }

    public static void main(String[] args){
        test01();
    }
}
