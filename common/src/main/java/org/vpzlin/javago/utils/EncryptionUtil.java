package org.vpzlin.javago.utils;

import java.security.MessageDigest;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class EncryptionUtil {
    /**
     * MD5
     */
    public static String md5EncodeAscII(String text){
        return encrypt(text, "MD5", "ASCII");
    }

    public static String md5EncodeGBK(String text){
        return encrypt(text, "MD5", "GBK");
    }

    public static String md5EncodeUTF8(String text){
        return encrypt(text, "MD5", "UTF-8");
    }

    /**
     * SHA-1
     */
    public static String sha1EncodeAscII(String text){
        return encrypt(text, "SHA-1", "ASCII");
    }

    public static String sha1EncodeGBK(String text){
        return encrypt(text, "SHA-1", "GBK");
    }

    public static String sha1EncodeUTF8(String text){
        return encrypt(text, "SHA-1", "UTF-8");
    }

    /**
     * SHA-224
     */
    public static String sha224EncodeAscII(String text){
        return encrypt(text, "SHA-224", "ASCII");
    }

    public static String sha224EncodeGBK(String text){
        return encrypt(text, "SHA-224", "GBK");
    }

    public static String sha224EncodeUTF8(String text){
        return encrypt(text, "SHA-224", "UTF-8");
    }

    /**
     * SHA-256
     */
    public static String sha256EncodeAscII(String text){
        return encrypt(text, "SHA-256", "ASCII");
    }

    public static String sha256EncodeGBK(String text){
        return encrypt(text, "SHA-256", "GBK");
    }

    public static String sha256EncodeUTF8(String text){
        return encrypt(text, "SHA-256", "UTF-8");
    }

    /**
     * SHA-384
     */
    public static String sha384EncodeAscII(String text){
        return encrypt(text, "SHA-384", "ASCII");
    }

    public static String sha384EncodeGBK(String text){
        return encrypt(text, "SHA-384", "GBK");
    }

    public static String sha384EncodeUTF8(String text){
        return encrypt(text, "SHA-384", "UTF-8");
    }

    /**
     * SHA-512
     */
    public static String sha512EncodeAscII(String text){
        return encrypt(text, "SHA-512", "ASCII");
    }

    public static String sha512EncodeGBK(String text){
        return encrypt(text, "SHA-512", "GBK");
    }

    public static String sha512EncodeUTF8(String text){
        return encrypt(text, "SHA-512", "UTF-8");
    }

    /**
     * HMACMD5
     */
    public static String hmacMd5EncodeAscII(String text, String key){
        return encrypt(text, key, "HmacMD5", "ASCII");
    }

    public static String hmacMd5EncodeGBK(String text, String key){
        return encrypt(text, key, "HmacMD5", "GBK");
    }

    public static String hmacMd5EncodeUTF8(String text, String key){
        return encrypt(text, key, "HmacMD5", "UTF-8");
    }

    /**
     * HMACSHA-1
     */
    public static String hmacSha1EncodeAscII(String text, String key){
        return encrypt(text, key, "HmacSHA1", "ASCII");
    }

    public static String hmacSha1EncodeGBK(String text, String key){
        return encrypt(text, key, "HmacSHA1", "GBK");
    }

    public static String hmacSha1EncodeUTF8(String text, String key){
        return encrypt(text, key, "HmacSHA1", "UTF-8");
    }

    /**
     * HMACSHA-224
     */
    public static String hmacSha224EncodeAscII(String text, String key){
        return encrypt(text, key, "HmacSHA224", "ASCII");
    }

    public static String hmacSha224EncodeGBK(String text, String key){
        return encrypt(text, key, "HmacSHA224", "GBK");
    }

    public static String hmacSha224EncodeUTF8(String text, String key){
        return encrypt(text, key, "HmacSHA224", "UTF-8");
    }

    /**
     * HMACSHA-256
     */
    public static String hmacSha256EncodeAscII(String text, String key){
        return encrypt(text, key, "HmacSHA256", "ASCII");
    }

    public static String hmacSha256EncodeGBK(String text, String key){
        return encrypt(text, key, "HmacSHA256", "GBK");
    }

    public static String hmacSha256EncodeUTF8(String text, String key){
        return encrypt(text, key, "HmacSHA256", "UTF-8");
    }

    /**
     * HMACSHA-256
     */
    public static String hmacSha384EncodeAscII(String text, String key){
        return encrypt(text, key, "HmacSHA384", "ASCII");
    }

    public static String hmacSha384EncodeGBK(String text, String key){
        return encrypt(text, key, "HmacSHA384", "GBK");
    }

    public static String hmacSha384EncodeUTF8(String text, String key){
        return encrypt(text, key, "HmacSHA384", "UTF-8");
    }

    /**
     * HMACSHA-512
     */
    public static String hmacSha512EncodeAscII(String text, String key){
        return encrypt(text, key, "HmacSHA512", "ASCII");
    }

    public static String hmacSha512EncodeGBK(String text, String key){
        return encrypt(text, key, "HmacSHA512", "GBK");
    }

    public static String hmacSha512EncodeUTF8(String text, String key){
        return encrypt(text, key, "HmacSHA512", "UTF-8");
    }

    /**
     * get encrypted string
     * @param text the string which to be encrypted
     * @param key the key to encrypt text
     * @param encryptionType the encryption type
     * @param charSet the char set of source string
     * @return the encrypted string, return null if failed
     */
    private static String encrypt(String text, String key, String encryptionType, String charSet){
        try {
            Mac hmac = Mac.getInstance(encryptionType);
            SecretKeySpec secretKeySpec = new SecretKeySpec(key.getBytes(charSet), encryptionType);
            hmac.init(secretKeySpec);
            byte[] bytes = hmac.doFinal(text.getBytes(charSet));
            StringBuilder sb = new StringBuilder();
            for(byte byteItem: bytes){
                sb.append(Integer.toHexString((byteItem & 0xFF) | 0x100).substring(1, 3));
            }
            return sb.toString();
        }
        catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * get encrypted string
     * @param text the string which to be encrypted
     * @param encryptionType the encryption type
     * @param charSet the char set of source string
     * @return the encrypted string, return null if failed
     */
    private static String encrypt(String text, String encryptionType, String charSet){
        MessageDigest messageDigest;
        String encodeStr;

        try {
            messageDigest = MessageDigest.getInstance(encryptionType);
            messageDigest.update(text.getBytes(charSet));
            encodeStr = byte2Hex(messageDigest.digest());
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return encodeStr;
    }

    /**
     * get hex string from source bytes
     * @param bytes source bytes
     * @return hex string
     */
    private static String byte2Hex(byte[] bytes){
        StringBuilder stringBuffer = new StringBuilder();
        String temp;
        for(byte byteItem: bytes){
            temp = Integer.toHexString(byteItem & 0xFF);
            if(temp.length() == 1){
                stringBuffer.append("0");
            }
            stringBuffer.append(temp);
        }

        return stringBuffer.toString();
    }
}
