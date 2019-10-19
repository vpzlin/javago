import org.vpzlin.javago.util.EncryptionUtil;

public class TestEncrypt {
    public static void test01(){
        String text = "vpzlin";
        String key = "xm";
        System.out.println("source text = " + text);

        System.out.println("md5 result asc2 = " + EncryptionUtil.md5EncodeAscII(text));
        System.out.println("md5 result gbk  = " + EncryptionUtil.md5EncodeGBK(text));
        System.out.println("md5 result utf8 = " + EncryptionUtil.md5EncodeUTF8(text));

        System.out.println("sha1 result asc2 = " + EncryptionUtil.sha1EncodeAscII(text));
        System.out.println("sha1 result gbk  = " + EncryptionUtil.sha1EncodeGBK(text));
        System.out.println("sha1 result utf8 = " + EncryptionUtil.sha1EncodeUTF8(text));

        System.out.println("sha224 result asc2 = " + EncryptionUtil.sha224EncodeAscII(text));
        System.out.println("sha224 result gbk  = " + EncryptionUtil.sha224EncodeGBK(text));
        System.out.println("sha224 result utf8 = " + EncryptionUtil.sha224EncodeUTF8(text));

        System.out.println("sha256 result asc2 = " + EncryptionUtil.sha256EncodeAscII(text));
        System.out.println("sha256 result gbk  = " + EncryptionUtil.sha256EncodeGBK(text));
        System.out.println("sha256 result utf8 = " + EncryptionUtil.sha256EncodeUTF8(text));

        System.out.println("sha384 result asc2 = " + EncryptionUtil.sha384EncodeAscII(text));
        System.out.println("sha384 result gbk  = " + EncryptionUtil.sha384EncodeGBK(text));
        System.out.println("sha384 result utf8 = " + EncryptionUtil.sha384EncodeUTF8(text));

        System.out.println("sha512 result asc2 = " + EncryptionUtil.sha512EncodeAscII(text));
        System.out.println("sha512 result gbk  = " + EncryptionUtil.sha512EncodeGBK(text));
        System.out.println("sha512 result utf8 = " + EncryptionUtil.sha512EncodeUTF8(text));

        System.out.println("hmac sha1 result asc2 = " + EncryptionUtil.hmacSha1EncodeAscII(text, key));
        System.out.println("hmac sha1 result gbk  = " + EncryptionUtil.hmacSha1EncodeGBK(text, key));
        System.out.println("hmac sha1 result utf8 = " + EncryptionUtil.hmacSha1EncodeUTF8(text, key));

    }

    public static void main(String[] args){
        test01();
    }
}
