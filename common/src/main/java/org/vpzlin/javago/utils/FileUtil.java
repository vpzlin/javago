package org.vpzlin.javago.utils;

import java.io.File;
import java.util.HashMap;

public class FileUtil{
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
        codeMap.put(10101001, "Path doesn't exist.");
        codeMap.put(10101002, "Path isn't a file.");
        codeMap.put(10101003, "Path isn't a directory.");
        codeMap.put(10101004, "Path isn't hidden.");
        codeMap.put(10101005, "Path can't be read.");
        codeMap.put(10101006, "Path can't be wrote.");
        codeMap.put(10101007, "Path can't be executed.");
        codeMap.put(10101008, "Path can't be deleted.");
    }

    public static int exists(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        return 0;
    }

    public static int isFile(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.isFile()){
            return 1010102;
        }
        return 0;
    }

    public static int isDirectory(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.isDirectory()){
            return 1010103;
        }
        return 0;
    }

    public static int isHidden(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.isHidden()){
            return 1010104;
        }
        return 0;
    }

    /**
     * return 0 if path like "/opt/data/a.txt";
     * return 1010104 if path like "data/a.txt" or like "a.txt".
     */
    public static int isAbsolute(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.isAbsolute()){
            return 1010104;
        }
        return 0;
    }

    public static int canRead(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.canRead()){
            return 1010105;
        }
        return 0;
    }

    public static int canWrite(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.canWrite()){
            return 1010106;
        }
        return 0;
    }

    public static int canExecute(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.canExecute()){
            return 1010107;
        }
        return 0;
    }

    public static int delete(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.delete()){
            return 1010108;
        }
        return 0;
    }

    public static int deleteFile(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.isFile()){
            return 1010102;
        }
        if(!file.delete()){
            return 1010108;
        }
        return 0;
    }

    public static int deleteDirectory(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1010101;
        }
        if(!file.isDirectory()){
            return 1010103;
        }
        if(!file.delete()){
            return 1010108;
        }
        return 0;
    }
}
