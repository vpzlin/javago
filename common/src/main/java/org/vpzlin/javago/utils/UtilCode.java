package org.vpzlin.javago.utils;

import java.util.HashMap;

public class UtilCode {
    protected static HashMap<Integer, String> codeMap = new HashMap<Integer, String>();

    public static String getInfoByCode(int code){
        if(!codeMap.containsKey(code)){
            return null;
        }
        return codeMap.get(code);
    }

    static {
        /**
         * normal
         */
        codeMap.put(0, "success");

        /**
         * codes in FileUtil
         */
        codeMap.put(1010101, "path doesn't exist");
        codeMap.put(1010102, "path isn't a file");
        codeMap.put(1010103, "path isn't a directory");
        codeMap.put(1010104, "path isn't hidden");
        codeMap.put(1010105, "path can't be read");
        codeMap.put(1010106, "path can't be wrote");
        codeMap.put(1010107, "path can't be executed");
        codeMap.put(1010108, "path can't be deleted");

    }
}
