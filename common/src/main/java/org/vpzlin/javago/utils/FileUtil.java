package org.vpzlin.javago.utils;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FileUtil{
    public static Result exists(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(true, true, String.format("Path [%s] exists.", path));
        }
        else {
            return Result.getResult(false, false, String.format("Path [%s] doesn't exist.", path));
        }
    }

    public static Result isFile(String path){

        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, false, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.isFile()){
            return Result.getResult(true, true, String.format("Path [%s] is a file.", path));
        }
        else{
            return Result.getResult(false, true, String.format("Path [%s] isn't a file.", path));
        }
    }

    public static Result isDirectory(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, false, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.isDirectory()){
            return Result.getResult(true, true, String.format("Path [%s] is a directory.", path));
        }
        else{
            return Result.getResult(false, false, String.format("Path [%s] isn't a directory.", path));
        }
    }

    public static Result isHidden(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, false, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.isHidden()){
            return Result.getResult(true, true, String.format("Path [%s] is hidden.", path));
        }
        else{
            return Result.getResult(false, false, String.format("Path [%s] isn't hidden.", path));
        }
    }

    /**
     * path like "/opt/data/a.txt" is absolute;
     * path like "data/a.txt" or like "a.txt" isn't absolute.
     */
    public static Result isAbsolute(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, false, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.isAbsolute()){
            return Result.getResult(true, true, String.format("Path [%s] is absolute.", path));
        }
        else{
            return Result.getResult(false, false, String.format("Path [%s] isn't hidden.", path));
        }
    }

    public static Result canRead(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, false, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.canRead()){
            return Result.getResult(true, true, String.format("Path [%s] can be read.", path));
        }
        else{
            return Result.getResult(false, false, String.format("Path [%s] can't be read.", path));
        }
    }

    public static Result canWrite(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, false, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.canWrite()){
            return Result.getResult(true, true, String.format("Path [%s] can be wrote.", path));
        }
        else{
            return Result.getResult(false, false, String.format("Path [%s] can't be wrote.", path));
        }
    }

    public static Result canExecute(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, false, String.format("Path [%s] doesn't exist.", path));
        }

        if(file.canExecute()){
            return Result.getResult(true, true, String.format("Path [%s] can be executed.", path));
        }
        else{
            return Result.getResult(false, false, String.format("Path [%s] can't be executed.", path));
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

    public static Result mkdirP(String path){
        return mkdirs(path);
    }

    /**
     * @return Result.data is a String object
     */
    public static Result getParentPath(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get parent path of path [%s], it doesn't exist.", path));
        }
        return Result.getResult(true, file.getParent(), String.format("Got parent path [%s] of path [%s].", file.getParent(), path));
    }

    /**
     * @return Result.data is a List<String> object
     */
    public static Result list(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get list of path [%s], it doesn't exist.", path));
        }
        if(file.isFile()){
            return Result.getResult(false, null, String.format("Failed to get list of path [%s], it's a file path.", path));
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
     * @return Result.data is a <String> object
     */
    public static Result listFiles(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get file list of path [%s], it doesn't exist.", path));
        }
        if(file.isFile()){
            return Result.getResult(false, null, String.format("Failed to get list of path [%s], it's a file path.", path));
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
     * @return Result.data is a List<String> object
     */
    public static Result listDirectories(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get directories list of path [%s], it doesn't exist.", path));
        }
        if(file.isFile()){
            return Result.getResult(false, null, String.format("Failed to get list of path [%s], it's a file path.", path));
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
     * @return Result.data is a List<String> object
     */
    public static Result listHiddens(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get hidden list of path [%s], it doesn't exist.", path));
        }
        if(file.isFile()){
            return Result.getResult(false, null, String.format("Failed to get list of path [%s], it's a file path.", path));
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

    /**
     * @return Result.data is a String object
     */
    public static Result getFileName(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get file name of path [%s], it doesn't exist.", path));
        }

        return Result.getResult(true, file.getName(), String.format("Got file name [%s] of path [%s].", file.getName(), path));
    }

    /**
     * @return Result.data is a String object
     */
    public static Result getFileExtension(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get file extension of path [%s], it doesn't exist.", path));
        }
        String fileName = file.getName();
        String fileExtension = fileName.contains(".") ? fileName.substring(fileName.lastIndexOf(".") + 1).trim() : "";
        if(fileExtension.length() == 0){
            return Result.getResult(false, "", String.format("Failed to get file extension of path [%s], it hasn't a extension.", path));
        }
        else {
            return Result.getResult(true, fileExtension, String.format("Got file extension [%s] of path [%s].", fileExtension, path));
        }
    }

    /**
     * @return Result.data is a String object
     */
    public static Result getFileNameWithoutExtension(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get file name without extension of path [%s], it doesn't exist.", path));
        }
        String fileName = file.getName();
        int idxDot = fileName.lastIndexOf(".");
        if(idxDot > 0 && fileName.substring(0, idxDot).trim().length() != 0){
            fileName = fileName.substring(0, idxDot);
        }
        return Result.getResult(true, fileName, String.format("Got file name without extension [%s] of path [%s].", fileName, path));
    }

    /**
     * @return Result.data is a String object
     */
    public static Result getAbsolutePath(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get absolute path of path [%s], it doesn't exist.", path));
        }

        return Result.getResult(true, file.getAbsolutePath(), String.format("Got absolute path [%s] of path [%s].", file.getAbsolutePath(), path));
    }

    /**
     * @return Result.data is a String object
     */
    public static Result getFullPath(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get full path of path [%s], it doesn't exist.", path));
        }

        return Result.getResult(true, file.getAbsolutePath(), String.format("Got full path [%s] of path [%s].", file.getAbsolutePath(), path));
    }

    public static Result rename(String path, String newFileName){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to rename to [%s], the source path [%s] doesn't exist.", newFileName, path));
        }

        File newFile = new File(newFileName);
        String parentPath = newFile.getParent();
        String newPath;
        if(parentPath != null && parentPath.trim().length() > 0){
            newPath = file.getParent() + File.separator + newFile.getName();
        }
        else {
            newPath = newFileName;
        }

        if(newFile.exists()){
            return Result.getResult(false, null, String.format("Failed to rename to [%s], the rename target [%s] already exists.", newFileName, newPath));
        }

        if(file.renameTo(newFile)){
            return Result.getResult(true, null, String.format("Renamed to [%s], the rename target is [%s].", newFileName, newPath));
        }
        else{
            return Result.getResult(false, null, String.format("Failed to rename to [%s], the rename source is [%s].", newFileName, path));
        }
    }

    public static Result move(String sourcePath, String targetPath){
        File sourceFile = new File(sourcePath);
        if(!sourceFile.exists()){
            return Result.getResult(false, null, String.format("Failed to move source [%s] to target [%s], the source doesn't exist.", sourcePath, targetPath));
        }

        File targetFile = new File(targetPath);
        if(targetFile.exists()){
            return Result.getResult(false, null, String.format("Failed to move source [%s] to target [%s], the target already exists.", sourcePath, targetPath));
        }

        if(sourceFile.renameTo(targetFile)){
            return Result.getResult(true, null, String.format("Moved source [%s] to target [%s].", sourcePath, targetPath));
        }
        else {
            return Result.getResult(false, null, String.format("Failed to move source [%s] to target [%s].", sourcePath, targetPath));
        }
    }

    public static Result copy(String sourcePath, String targetPath){
        File sourceFile = new File(sourcePath);
        if(!sourceFile.exists()){
            return Result.getResult(false, null, String.format("Failed to copy source [%s] to target [%s], the source doesn't exist.", sourcePath, targetPath));
        }

        File targetFile = new File(targetPath);
        if(targetFile.exists()){
            return Result.getResult(false, null, String.format("Failed to copy source [%s] to target [%s], the target already exists.", sourcePath, targetPath));
        }

        try {
            FileInputStream fileInputStream = new FileInputStream(sourceFile);
            FileOutputStream fileOutputStream = new FileOutputStream(targetFile);
            byte[] bytes = new byte[1024];
            int i = 0;
            while ((i = fileInputStream.read(bytes)) != -1) {
                fileOutputStream.write(bytes, 0, i);
            }
            fileInputStream.close();
            fileOutputStream.close();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            return Result.getResult(false, null, String.format("Failed to copy source [%s] to target [%s], more info = [%s].", sourcePath, targetPath, e.getMessage()));
        }

        return Result.getResult(true, null, String.format("Copied source [%s] to target [%s].", sourcePath, targetPath));
    }

    public static Result appendText(String path, String text){
        return null;
    }

    public static Result writeText(String path, String text, boolean overwrite){
        return null;
    }

    /**
     * @return Result.data is a String object
     */
    public static Result readText(String path){
        return null;
    }

    /**
     * get text by index range of bytes
     * @param path the file path
     * @param idxBeginByte the finger point of begin byte index, the first char's index is [1]
     * @param idxEndByte the finger point of end byte index, this must be large to the parameter [idxBeginByte]
     * @return Result.data is a String object
     */
    public static Result readText(String path, int idxBeginByte, int idxEndByte, String charsetName){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to read random text from [%s], it doesn't exist.", path));
        }
        if(!file.isFile()){
            return Result.getResult(false, null, String.format("Failed to read random text from [%s], it isn't a file.", path));
        }

        /**
         * about indices
         */
        if(idxBeginByte < 0){
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], the begin byte index [%d] must not be less than number [0].", path, idxBeginByte));
        }
        if(idxEndByte < 1){
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], the end byte index [%d] must not be less than number [1].", path, idxEndByte));
        }
        if(idxBeginByte >= idxEndByte){
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], the begin byte index [%d] must be less than the end byte index [%d].", path, idxBeginByte, idxEndByte));
        }

        if(charsetName == null || charsetName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to read random text from [%s], the parameter charsetName must not be null or empty.", path));
        }

        // do read
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r");
            long fileSize = randomAccessFile.length();
            if(idxBeginByte > fileSize){
                return Result.getResult(false, null, String.format("Failed to read random text from file [%s], the begin byte index [%d] is overflowed than the file size [%d].", path, idxBeginByte, fileSize));
            }

            idxEndByte = (long)idxEndByte > fileSize ? (int)fileSize : idxEndByte;
            int size = (long)(idxEndByte - idxBeginByte) > randomAccessFile.length() ? (int)randomAccessFile.length() : (idxEndByte - idxBeginByte);
            byte[] bytes = new byte[size];
            randomAccessFile.seek(idxBeginByte);
            randomAccessFile.read(bytes);
            randomAccessFile.close();
            String textRead = new String(bytes, charsetName);
            return Result.getResult(true, textRead, String.format("The random text read from file [%s] is [%s].", path, textRead));
        } catch (FileNotFoundException e) {
            return Result.getResult(false, null, String.format("Failed to read random text from file [%s], it doesn't exist.", path));
        } catch (IOException e) {
            return Result.getResult(false, null, String.format("Failed to read random text from file [%s], more info = [%s].", path, e.getMessage()));
        }
    }

    public static Result readTextUtf8(String path, int idxBeginByte, int idxEndByte){
        return readText(path, idxBeginByte, idxEndByte, "UTF8");
    }

    public static Result readTextGbk(String path, int idxBeginByte, int idxEndByte){
        return readText(path, idxBeginByte, idxEndByte, "GBK");
    }

    /**
     * @return Result.data is a String object
     */
    public static Result readTextLines(String path, int beginLine, int endLine){
        return null;
    }

    /**
     * @return Result.data is a byte[] object
     */
    public static Result readBytes(String path){
        return null;
    }
}
