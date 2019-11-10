import org.vpzlin.javago.utils.EncryptionUtil;

public class TestEncrypt {
    public static void test01(){
        String text = "vpzlin";
        String key = "xm";
        System.out.println("source text = " + text);

        System.out.println("md5 result gbk  = " + EncryptionUtil.md5EncodeGBK(text));
        System.out.println("md5 result utf8 = " + EncryptionUtil.md5EncodeUTF8(text));

        System.out.println("sha1 result gbk  = " + EncryptionUtil.sha1EncodeGBK(text));
        System.out.println("sha1 result utf8 = " + EncryptionUtil.sha1EncodeUTF8(text));

        System.out.println("sha224 result gbk  = " + EncryptionUtil.sha224EncodeGBK(text));
        System.out.println("sha224 result utf8 = " + EncryptionUtil.sha224EncodeUTF8(text));

        System.out.println("sha256 result gbk  = " + EncryptionUtil.sha256EncodeGBK(text));
        System.out.println("sha256 result utf8 = " + EncryptionUtil.sha256EncodeUTF8(text));

        System.out.println("sha384 result gbk  = " + EncryptionUtil.sha384EncodeGBK(text));
        System.out.println("sha384 result utf8 = " + EncryptionUtil.sha384EncodeUTF8(text));

        System.out.println("sha512 result gbk  = " + EncryptionUtil.sha512EncodeGBK(text));
        System.out.println("sha512 result utf8 = " + EncryptionUtil.sha512EncodeUTF8(text));

        System.out.println("hmac md5 result gbk  = " + EncryptionUtil.hmacMd5EncodeGBK(text, key));
        System.out.println("hmac md5 result utf8 = " + EncryptionUtil.hmacMd5EncodeUTF8(text, key));

        System.out.println("hmac sha1 result gbk  = " + EncryptionUtil.hmacSha1EncodeGBK(text, key));
        System.out.println("hmac sha1 result utf8 = " + EncryptionUtil.hmacSha1EncodeUTF8(text, key));

        System.out.println("hmac sha224 result gbk  = " + EncryptionUtil.hmacSha224EncodeGBK(text, key));
        System.out.println("hmac sha224 result utf8 = " + EncryptionUtil.hmacSha224EncodeUTF8(text, key));

        System.out.println("hmac sha256 result gbk  = " + EncryptionUtil.hmacSha256EncodeGBK(text, key));
        System.out.println("hmac sha256 result utf8 = " + EncryptionUtil.hmacSha256EncodeUTF8(text, key));

        System.out.println("hmac sha384 result gbk  = " + EncryptionUtil.hmacSha384EncodeGBK(text, key));
        System.out.println("hmac sha384 result utf8 = " + EncryptionUtil.hmacSha384EncodeUTF8(text, key));

        System.out.println("hmac sha512 result gbk  = " + EncryptionUtil.hmacSha512EncodeGBK(text, key));
        System.out.println("hmac sha512 result utf8 = " + EncryptionUtil.hmacSha512EncodeUTF8(text, key));
    }

    public static void main(String[] args){
        test01();
    }
}
