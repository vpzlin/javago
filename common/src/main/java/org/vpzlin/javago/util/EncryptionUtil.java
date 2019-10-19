package org.vpzlin.javago.util;

import java.security.MessageDigest;

public class EncryptionUtil {
    /**
     * MD5
     */
    public static String md5EncodeAscII(String gbkString){
        return encrypt(gbkString, "MD5", "ASCII");
    }

    public static String md5EncodeGBK(String gbkString){
        return encrypt(gbkString, "MD5", "GBK");
    }

    public static String md5EncodeUTF8(String gbkString){
        return encrypt(gbkString, "MD5", "UTF-8");
    }

    /**
     * SHA-1
     */
    public static String sha1EncodeAscII(String gbkString){
        return encrypt(gbkString, "SHA-1", "ASCII");
    }

    public static String sha1EncodeGBK(String gbkString){
        return encrypt(gbkString, "SHA-1", "GBK");
    }

    public static String sha1EncodeUTF8(String gbkString){
        return encrypt(gbkString, "SHA-1", "UTF-8");
    }

    /**
     * SHA-224
     */
    public static String sha224EncodeAscII(String gbkString){
        return encrypt(gbkString, "SHA-224", "ASCII");
    }

    public static String sha224EncodeGBK(String gbkString){
        return encrypt(gbkString, "SHA-224", "GBK");
    }

    public static String sha224EncodeUTF8(String gbkString){
        return encrypt(gbkString, "SHA-224", "UTF-8");
    }

    /**
     * SHA-256
     */
    public static String sha256EncodeAscII(String gbkString){
        return encrypt(gbkString, "SHA-256", "ASCII");
    }

    public static String sha256EncodeGBK(String gbkString){
        return encrypt(gbkString, "SHA-256", "GBK");
    }

    public static String sha256EncodeUTF8(String gbkString){
        return encrypt(gbkString, "SHA-256", "UTF-8");
    }

    /**
     * SHA-384
     */
    public static String sha384EncodeAscII(String gbkString){
        return encrypt(gbkString, "SHA-384", "ASCII");
    }

    public static String sha384EncodeGBK(String gbkString){
        return encrypt(gbkString, "SHA-384", "GBK");
    }

    public static String sha384EncodeUTF8(String gbkString){
        return encrypt(gbkString, "SHA-384", "UTF-8");
    }

    /**
     * SHA-512
     */
    public static String sha512EncodeAscII(String gbkString){
        return encrypt(gbkString, "SHA-512", "ASCII");
    }

    public static String sha512EncodeGBK(String gbkString){
        return encrypt(gbkString, "SHA-512", "GBK");
    }

    public static String sha512EncodeUTF8(String gbkString){
        return encrypt(gbkString, "SHA-512", "UTF-8");
    }


    /**
     * get encrypted string
     * @param sourceString the string which to be encrypted
     * @param encryptionType the encryption type
     * @param charSet the char set of source string
     * @return the encrypted string, return null if failed
     */
    private static String encrypt(String sourceString, String encryptionType, String charSet){
        MessageDigest messageDigest;
        String encodeStr;
        try {
            messageDigest = MessageDigest.getInstance(encryptionType);
            messageDigest.update(sourceString.getBytes(charSet));
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
        for(byte b: bytes){
            temp = Integer.toHexString(b & 0xFF);
            if(temp.length() == 1){
                stringBuffer.append("0");
            }
            stringBuffer.append(temp);
        }
        return stringBuffer.toString();
    }
}
