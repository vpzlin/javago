package org.vpzlin.javago.utils;

import java.io.File;

/**
 * return codes of functions:
 *           0: success
 *     1010101: path doesn't exist
 *     1010102: path isn't a file
 *     1010103: path isn't a directory
 *     1010104: path isn't hidden
 *     1010105: path can't be read
 *     1010106: path can't be wrote
 *     1010107: path can't be executed
 *     1010108: path can't be deleted
 */
public class FileUtil extends UtilCode{
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
