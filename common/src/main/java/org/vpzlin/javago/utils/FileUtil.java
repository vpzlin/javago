package org.vpzlin.javago.utils;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FileUtil{
    public static Result exists(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(true, null, String.format("Path [%s] exists.", path));
        }
        else {
            return Result.getResult(false, null, String.format("Path [%s] doesn't exist.", path));
        }
    }

    public static Result isFile(String path){

        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.isFile()){
            return Result.getResult(true, null, String.format("Path [%s] is a file.", path));
        }
        else{
            return Result.getResult(false, null, String.format("Path [%s] isn't a file.", path));
        }
    }

    public static Result isDirectory(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.isDirectory()){
            return Result.getResult(true, null, String.format("Path [%s] is a directory.", path));
        }
        else{
            return Result.getResult(false, null, String.format("Path [%s] isn't a directory.", path));
        }
    }

    public static Result isHidden(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.isHidden()){
            return Result.getResult(true, null, String.format("Path [%s] is hidden.", path));
        }
        else{
            return Result.getResult(false, null, String.format("Path [%s] isn't hidden.", path));
        }
    }

    /**
     * path like "/opt/data/a.txt" is absolute;
     * path like "data/a.txt" or like "a.txt" isn't absolute.
     */
    public static Result isAbsolute(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.isAbsolute()){
            return Result.getResult(true, null, String.format("Path [%s] is absolute.", path));
        }
        else{
            return Result.getResult(false, null, String.format("Path [%s] isn't hidden.", path));
        }
    }

    public static Result canRead(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.canRead()){
            return Result.getResult(true, null, String.format("Path [%s] can be read.", path));
        }
        else{
            return Result.getResult(false, null, String.format("Path [%s] can't be read.", path));
        }
    }

    public static Result canWrite(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.canWrite()){
            return Result.getResult(true, null, String.format("Path [%s] can be wrote.", path));
        }
        else{
            return Result.getResult(false, null, String.format("Path [%s] can't be wrote.", path));
        }
    }

    public static Result canExecute(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.canExecute()){
            return Result.getResult(true, null, String.format("Path [%s] can be executed.", path));
        }
        else{
            return Result.getResult(false, null, String.format("Path [%s] can't be executed.", path));
        }
    }

    public static Result rm(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to delete [%s], it doesn't exist.", path));
        }

        if(file.delete()){
            return Result.getResult(true, null, String.format("Deleted [%s].", path));
        }
        else {
            return Result.getResult(false, null, String.format("Failed to delete [%s].", path));
        }
    }

    public static Result rmFile(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to delete [%s], it doesn't exist.", path));
        }

        if(!file.isFile()){
            return Result.getResult(false, null, String.format("Failed to delete file [%s], it isn't a file.", path));
        }

        if(file.delete()){
            return Result.getResult(true, null, String.format("Deleted file [%s].", path));
        }
        else {
            return Result.getResult(false, null, String.format("Failed to delete file [%s].", path));
        }
    }

    public static Result rmDir(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to delete [%s], it doesn't exist.", path));
        }
        if(!file.isDirectory()){
            return Result.getResult(false, null, String.format("Failed to delete [%s], it isn't a directory.", path));
        }

        if(file.delete()){
            return Result.getResult(true, null, String.format("Deleted directory [%s].", path));
        }
        else {
            return Result.getResult(false, null, String.format("Failed to delete directory [%s].", path));
        }
    }
    
    public static Result createNewFile(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(false, null, String.format("Failed to create new file [%s], it already exists.", path));
        }

        try {
            if(file.createNewFile()){
                return Result.getResult(true, null, String.format("Created new file [%s].", path));
            }
            else {
                return Result.getResult(false, null, String.format("Failed to create new file [%s].", path));
            }
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to create new file [%s]. %s", path, e.getMessage()));
        }
    }

    public static Result touch(String path){
        File file = new File(path);
        if(file.exists()){
            long currentTimeMillis = System.currentTimeMillis();
            long timeMillis = file.lastModified();
            if(file.setLastModified(currentTimeMillis)){
                return Result.getResult(true, null, String.format("Touched path [%s], updated last modified time from timestamp [%s] to [%s].", path, timeMillis, currentTimeMillis));
            }
            else {
                return Result.getResult(false, null, String.format("Failed to touch path [%s].", path));
            }
        }
        else {
            try {
                if(file.createNewFile()){
                    return Result.getResult(true, null, String.format("Touched new file [%s].", path));
                }
                else {
                    return Result.getResult(false, null, String.format("Failed to touch new file [%s].", path));
                }
            }
            catch (Exception e){
                return Result.getResult(false, null, String.format("Failed to touch new file [%s]. %s", path, e.getMessage()));
            }
        }
    }

    public static Result mkdir(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(false, null, String.format("Failed to mkdir [%s], it already exists.", path));
        }

        if(file.mkdir()){
            return Result.getResult(true, null, String.format("Succeeded to mkdir [%s].", path));
        }
        else {
            return Result.getResult(false, null, String.format("Failed to mkdir [%s].", path));
        }
    }

    /**
     * create recursive directory path
     */
    public static Result mkdirs(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(false, null, String.format("Failed to mkdir [%s], it already exists.", path));
        }

        if(file.mkdirs()){
            return Result.getResult(true, null, String.format("Succeeded to mkdirs [%s].", path));
        }
        else {
            return Result.getResult(false, null, String.format("Failed to mkdirs [%s].", path));
        }
    }

    /**
     * @return Result.data is a String object
     */
    public static Result getFileName(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(false, null, String.format("Failed to get file name of path [%s], it doesn't exist.", path));
        }

        return Result.getResult(true, file.getName(), String.format("Got file name [%s] of path [%s].", file.getName(), path));
    }

    /**
     * @return Result.data is a String object
     */
    public static Result getParentPath(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(false, null, String.format("Failed to get parent path of path [%s], it doesn't exist.", path));
        }
        return Result.getResult(true, file.getParent(), String.format("Got parent path [%s] of path [%s].", file.getParent(), path));
    }

    /**
     * @return Result.data is a List object
     */
    public static Result list(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(false, null, String.format("Failed to get list of path [%s], it doesn't exist.", path));
        }

        String[] argsList = file.list();
        List<String> list;
        if(argsList != null){
            list = Arrays.asList(argsList);
        }
        else {
            list = new LinkedList<>();
        }
        return Result.getResult(true, list, String.format("Got list of path [%s].", path));
    }

    /**
     * @return Result.data is a List object
     */
    public static Result listFiles(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(false, null, String.format("Failed to get file list of path [%s], it doesn't exist.", path));
        }

        String[] argsList = file.list();
        List<String> list;
        if(argsList != null){
            list = Arrays.asList(argsList);
        }
        else {
            list = new LinkedList<>();
        }

        // delete sub which is not file
        for(String subFilePath: list){
            File subFile = new File(subFilePath);
            if(!subFile.isFile()){
                list.remove(subFilePath);
            }
        }
        return Result.getResult(true, list, String.format("Got file list of path [%s].", path));
    }

    /**
     * @return Result.data is a List object
     */
    public static Result listDirectories(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(false, null, String.format("Failed to get directories list of path [%s], it doesn't exist.", path));
        }

        String[] argsList = file.list();
        List<String> list;
        if(argsList != null){
            list = Arrays.asList(argsList);
        }
        else {
            list = new LinkedList<>();
        }

        // delete object which is not directory
        for(String subFilePath: list){
            File subFile = new File(subFilePath);
            if(!subFile.isFile()){
                list.remove(subFilePath);
            }
        }
        return Result.getResult(true, list, String.format("Got directories list of path [%s].", path));
    }

    /**
     * @return Result.data is a List object
     */
    public static Result listHiddens(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(false, null, String.format("Failed to get hidden list of path [%s], it doesn't exist.", path));
        }

        String[] argsList = file.list();
        List<String> list;
        if(argsList != null){
            list = Arrays.asList(argsList);
        }
        else {
            list = new LinkedList<>();
        }

        // delete object which is not hidden
        for(String subFilePath: list){
            File subFile = new File(subFilePath);
            if(!subFile.isFile()){
                list.remove(subFilePath);
            }
        }
        return Result.getResult(true, list, String.format("Got hidden list of path [%s].", path));
    }
}
