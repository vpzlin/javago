package org.vpzlin.javago.utils;

import java.io.File;

/**
 * return codes of functions:
 *     0: success
 *     1: path doesn't exist
 *     2: path isn't a file
 *     3: path isn't a directory
 *     4: path isn't hidden
 *     5: path can't read
 *     6: path can't write
 *     7: path can't execute
 *     8: path can't delete
 */
public class FileUtil {
    public static int exists(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        return 0;
    }

    public static int isFile(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.isFile()){
            return 2;
        }
        return 0;
    }

    public static int isDirectory(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.isDirectory()){
            return 3;
        }
        return 0;
    }

    public static int isHidden(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.isHidden()){
            return 4;
        }
        return 0;
    }

    /**
     * return 0 if path like "/opt/data/a.txt";
     * return 4 if path like "data/a.txt" or like "a.txt".
     */
    public static int isAbsolute(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.isAbsolute()){
            return 4;
        }
        return 0;
    }

    public static int canRead(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.canRead()){
            return 5;
        }
        return 0;
    }

    public static int canWrite(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.canWrite()){
            return 6;
        }
        return 0;
    }

    public static int canExecute(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.canExecute()){
            return 7;
        }
        return 0;
    }

    public static int delete(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.delete()){
            return 8;
        }
        return 0;
    }

    public static int deleteFile(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.isFile()){
            return 2;
        }
        if(!file.delete()){
            return 8;
        }
        return 0;
    }

    public static int deleteDirectory(String path){
        File file = new File(path);
        if(!file.exists()){
            return 1;
        }
        if(!file.isDirectory()){
            return 3;
        }
        if(!file.delete()){
            return 8;
        }
        return 0;
    }
}
