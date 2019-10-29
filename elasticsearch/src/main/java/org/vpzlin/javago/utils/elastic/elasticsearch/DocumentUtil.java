package org.vpzlin.javago.utils.elastic.elasticsearch;

import java.util.HashMap;

public class DocumentUtil {
    protected static HashMap<Integer, String> codeMap = new HashMap<Integer, String>();

    public static String getInfoByCode(int code){
        if(!codeMap.containsKey(code)){
            return null;
        }
        return codeMap.get(code);
    }

    /**
     * init codes map
     */
    static {
        codeMap.put(10202001, "Document doesn't exist.");
        codeMap.put(10202002, "Document already exists.");
    }
}
