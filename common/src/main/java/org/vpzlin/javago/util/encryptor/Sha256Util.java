package org.vpzlin.javago.util.encryptor;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Sha256Util {
    /**
     * get sha-256 value from source text
     * @param str source text
     * @return sha-256 value. if it's failed, return null
     */
    public static String getSha256(String str){
        MessageDigest messageDigest;
        String encodeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(str.getBytes("UTF-8"));
            encodeStr = byte2Hex(messageDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        } catch (UnsupportedEncodingException e) {
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
        StringBuffer stringBuffer = new StringBuffer();
        String temp = null;
        for(int i = 0; i < bytes.length; i++){
            temp = Integer.toHexString(bytes[i] & 0xFF);
            if(temp.length() == 1){
                // 1得到1位的进行补0操作
                stringBuffer.append("0");
            }
            stringBuffer.append(temp);
        }
        return stringBuffer.toString();
    }
}
