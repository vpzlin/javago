package org.vpzlin.javago.utils;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FileUtil{
    /**
     * check path's status: exist
     * @param path the path of file or directory
     * @return Result.data is a boolean type which is equal to Result.isSuccess
     */
    public static Result exists(String path){
        File file = new File(path);
        if(file.exists()){
            return Result.getResult(true, true, String.format("Path [%s] exists.", path));
        }
        else {
            return Result.getResult(false, false, String.format("Path [%s] doesn't exist.", path));
        }
    }

    /**
     * check path's status: is a file
     * @param path the path of file
     * @return Result.data is a boolean type which is equal to Result.isSuccess
     */
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

    /**
     * check path's status: is a directory
     * @param path the path of directory
     * @return Result.data is a boolean type which is equal to Result.isSuccess
     */
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

    /**
     * check path's status: is hidden
     * @param path the path of file or directory
     * @return Result.data is a boolean type which is equal to Result.isSuccess
     */
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
     * check path's status: is a absolute path
     * @param path path like "/opt/data/a.txt" is absolute; path like "data/a.txt" or like "a.txt" isn't absolute.
     * @return Result.data is a boolean type which is equal to Result.isSuccess
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

    /**
     * check path's permission: read
     * @param path the path of file or directory
     * @return Result.data is a boolean type which is equal to Result.isSuccess
     */
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

    /**
     * check path's permission: write
     * @param path the path of file or directory
     * @return Result.data is a boolean type which is equal to Result.isSuccess
     */
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

    /**
     * check path's permission: execute
     * @param path the path of file or directory
     * @return Result.data is a boolean type which is equal to Result.isSuccess
     */
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

    /**
     * remove the path of file or directory
     * @param path the path of file or directory
     * @return
     */
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

    /**
     * remove the path of file
     * @param path the path of file
     * @return
     */
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

    /**
     * remove the path of directory
     * @param path the path of directory
     * @return
     */
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

    /**
     * create new file
     * @param path the path of file
     * @return
     */
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

    /**
     * touch file or directory
     * @param path the path of file or directory
     * @return
     */
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

    /**
     * create a directory
     * @param path the path of directory
     * @return
     */
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
     * create a recursive directory
     * @param path the path of directory
     * @return
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
     * create a recursive directory
     * @param path the path of directory
     * @return
     */
    public static Result mkdirP(String path){
        return mkdirs(path);
    }

    /**
     * get parent path
     * @param path the path which to get parent path of
     * @return Result.data is a String type
     */
    public static Result getParentPath(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get parent path of path [%s], it doesn't exist.", path));
        }
        return Result.getResult(true, file.getParent(), String.format("Got parent path [%s] of path [%s].", file.getParent(), path));
    }

    /**
     * get sub files and directories list of directory
     * @param path the path of directory
     * @return Result.data is a List<String> type
     */
    public static Result list(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get sub files and directories list of path [%s], it doesn't exist.", path));
        }

        String[] argsList = file.list();
        List<String> list;
        if(argsList != null){
            list = Arrays.asList(argsList);
        }
        else {
            list = new LinkedList<>();
        }
        return Result.getResult(true, list, String.format("Got sub files and directories of path [%s].", path));
    }

    /**
     * get sub files list of directory
     * @param path the path of directory
     * @return Result.data is a List<String> type
     */
    public static Result listFiles(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get sub files list of path [%s], it doesn't exist.", path));
        }

        String[] argsList = file.list();
        List<String> list;
        if(argsList != null){
            list = Arrays.asList(argsList);
        }
        else {
            list = new LinkedList<>();
        }

        // delete sub object which is not a file
        for(String subFilePath: list){
            File subFile = new File(subFilePath);
            if(!subFile.isFile()){
                list.remove(subFilePath);
            }
        }
        return Result.getResult(true, list, String.format("Got sub files list of path [%s].", path));
    }

    /**
     * get sub directories list of directory
     * @param path the path of directory
     * @return Result.data is a List<String> type
     */
    public static Result listDirectories(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get sub directories list of path [%s], it doesn't exist.", path));
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
            if(!subFile.isDirectory()){
                list.remove(subFilePath);
            }
        }
        return Result.getResult(true, list, String.format("Got sub directories list of path [%s].", path));
    }

    /**
     * get sub file and directory of directory which is hidden
     * @param path the path of directory
     * @return Result.data is a List<String> type
     */
    public static Result listHidden(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get hidden files and directories list of path [%s], it doesn't exist.", path));
        }

        String[] argsList = file.list();
        List<String> list;
        if(argsList != null){
            list = Arrays.asList(argsList);
        }
        else {
            list = new LinkedList<>();
        }

        // delete file or directory which is not hidden
        for(String subFilePath: list){
            File subFile = new File(subFilePath);
            if(!subFile.isHidden()){
                list.remove(subFilePath);
            }
        }
        return Result.getResult(true, list, String.format("Got hidden files and directories list of path [%s].", path));
    }

    /**
     * get sub files which are hidden
     * @param path the path of directory
     * @return Result.data is a List<String> type
     */
    public static Result listHiddenFiles(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get hidden files list of path [%s], it doesn't exist.", path));
        }

        String[] argsList = file.list();
        List<String> list;
        if(argsList != null){
            list = Arrays.asList(argsList);
        }
        else {
            list = new LinkedList<>();
        }

        // delete file which is not hidden
        for(String subFilePath: list){
            File subFile = new File(subFilePath);
            if(!subFile.isHidden() || !subFile.isFile()){
                list.remove(subFilePath);
            }
        }
        return Result.getResult(true, list, String.format("Got hidden files list of path [%s].", path));
    }

    /**
     * get sub directories which are hidden
     * @param path the path of directory
     * @return Result.data is a List<String> type
     */
    public static Result listHiddenDirectories(String path){
        File file = new File(path);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to get hidden directories list of path [%s], it doesn't exist.", path));
        }

        String[] argsList = file.list();
        List<String> list;
        if(argsList != null){
            list = Arrays.asList(argsList);
        }
        else {
            list = new LinkedList<>();
        }

        // delete file which is not hidden
        for(String subFilePath: list){
            File subFile = new File(subFilePath);
            if(!subFile.isHidden() || !subFile.isDirectory()){
                list.remove(subFilePath);
            }
        }
        return Result.getResult(true, list, String.format("Got hidden directories list of path [%s].", path));
    }

    /**
     * get file name of file or directory
     * @param path the path of file or directory
     * @return Result.data is a String type
     */
    public static Result getFileName(String path){
        File file = new File(path);
        String filename = file.getName();

        return Result.getResult(true, filename, String.format("Got file name [%s] of path [%s].", filename, path));
    }

    /**
     * get extension of file
     * @param path the path of file or directory
     * @return Result.data is a String type
     */
    public static Result getFileExtension(String path){
        File file = new File(path);

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
     * get file name without extension
     * @param path the path of file
     * @return Result.data is a String type
     */
    public static Result getFileNameWithoutExtension(String path){
        File file = new File(path);

        String fileName = file.getName();
        int idxDot = fileName.lastIndexOf(".");
        if(idxDot > 0 && fileName.substring(0, idxDot).trim().length() != 0){
            fileName = fileName.substring(0, idxDot);
        }
        return Result.getResult(true, fileName, String.format("Got file name without extension [%s] of path [%s].", fileName, path));
    }

    /**
     * get absolute path
     * @param path the path of file or directory
     * @return Result.data is a String type
     */
    public static Result getAbsolutePath(String path){
        File file = new File(path);

        String absolutePath = file.getAbsolutePath();
        return Result.getResult(true, absolutePath, String.format("Got absolute path [%s] of path [%s].", absolutePath, path));
    }

    /**
     * get full path
     * @param path the path of file or directory
     * @return Result.data is a String type
     */
    public static Result getFullPath(String path){
        return getAbsolutePath(path);
    }

    /**
     * rename file or directory
     * @param filePath the path of file which to be renamed
     * @param newFileName the new file name which to be named to
     * @return
     */
    public static Result rename(String filePath, String newFileName){
        File file = new File(filePath);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to rename to new file name [%s], the path [%s] doesn't exist.", newFileName, filePath));
        }

        File newFile = new File(newFileName);
        if(newFile.getParent().contains(File.separator)){
            return Result.getResult(false, null, String.format("Failed to rename to new file name [%s], the new file name contains a parent path.", newFileName));
        }

        String newFilePath = file.getPath() + File.separator + newFileName.trim();
        newFile = new File(newFilePath);
        if(newFile.exists()){
            return Result.getResult(false, null, String.format("Failed to rename to new file name [%s], the new file [%s] already exists.", newFileName, newFilePath));
        }

        if(file.renameTo(newFile)){
            return Result.getResult(true, null, String.format("Renamed file [%s] to [%s] with new file name [%s].", filePath, newFilePath, newFileName));
        }
        else {
            return Result.getResult(false, null, String.format("Failed to renamed file [%s] to [%s] with new file name [%s].", filePath, newFilePath, newFileName));
        }
    }

    /**
     * move file to new path
     * @param sourcePath the path of source file
     * @param targetPath the path of target file
     * @return
     */
    public static Result move(String sourcePath, String targetPath){
        File sourceFile = new File(sourcePath);
        if(!sourceFile.exists()){
            return Result.getResult(false, null, String.format("Failed to move source file [%s] to target path [%s], the source file doesn't exist.", sourcePath, targetPath));
        }

        File targetFile = new File(targetPath);
        if(targetFile.exists()){
            return Result.getResult(false, null, String.format("Failed to move source file [%s] to target path [%s], the target path already exists.", sourcePath, targetPath));
        }

        if(sourceFile.renameTo(targetFile)){
            return Result.getResult(true, null, String.format("Moved source file [%s] to target path [%s].", sourcePath, targetPath));
        }
        else {
            return Result.getResult(false, null, String.format("Failed to move source file [%s] to target path [%s].", sourcePath, targetPath));
        }
    }

    /**
     * copy file to new path
     * @param sourcePath the path of source file
     * @param targetPath the path of target file
     * @return
     */
    public static Result copy(String sourcePath, String targetPath){
        File sourceFile = new File(sourcePath);
        if(!sourceFile.exists()){
            return Result.getResult(false, null, String.format("Failed to copy source file [%s] to target file [%s], the source file doesn't exist.", sourcePath, targetPath));
        }

        File targetFile = new File(targetPath);
        if(targetFile.exists()){
            return Result.getResult(false, null, String.format("Failed to copy source file [%s] to target file [%s], the target file already exists.", sourcePath, targetPath));
        }

        try {
            FileInputStream fileInputStream = new FileInputStream(sourceFile);
            FileOutputStream fileOutputStream = new FileOutputStream(targetFile);
            byte[] bytes = new byte[1024];
            int i;
            while ((i = fileInputStream.read(bytes)) != -1) {
                fileOutputStream.write(bytes, 0, i);
            }
            fileInputStream.close();
            fileOutputStream.close();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            return Result.getResult(false, null, String.format("Failed to copy source file [%s] to target file [%s], more info = [%s].", sourcePath, targetPath, e.getMessage()));
        }

        return Result.getResult(true, null, String.format("Copied source file [%s] to target file [%s].", sourcePath, targetPath));
    }

    /**
     * read text by index range of bytes
     * @param filePath the file path
     * @param idxBeginByte the finger point of begin byte index, the first char's index is [1]
     * @param idxEndByte the finger point of end byte index, this must be large to the parameter [idxBeginByte]
     * @param charsetName the charset name of text file
     * @return Result.data is a String type
     */
    public static Result readText(String filePath, int idxBeginByte, int idxEndByte, String charsetName){
        File file = new File(filePath);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to read random text from [%s], it doesn't exist.", filePath));
        }
        if(!file.isFile()){
            return Result.getResult(false, null, String.format("Failed to read random text from [%s], it isn't a file.", filePath));
        }

        /**
         * about indices
         */
        if(idxBeginByte < 0){
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], the begin byte index [%d] must not be less than number [0].", filePath, idxBeginByte));
        }
        if(idxEndByte < 1){
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], the end byte index [%d] must not be less than number [1].", filePath, idxEndByte));
        }
        if(idxBeginByte >= idxEndByte){
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], the begin byte index [%d] must be less than the end byte index [%d].", filePath, idxBeginByte, idxEndByte));
        }

        if(charsetName == null || charsetName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to read random text from [%s], the parameter charsetName must not be null or empty.", filePath));
        }

        // do read
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r");
            long fileSize = randomAccessFile.length();
            if(idxBeginByte > fileSize){
                return Result.getResult(false, null, String.format("Failed to read random text from file [%s], the begin byte index [%d] is overflowed than the file size [%d].", filePath, idxBeginByte, fileSize));
            }

            idxEndByte = (long)idxEndByte > fileSize ? (int)fileSize : idxEndByte;
            int size = (long)(idxEndByte - idxBeginByte) > randomAccessFile.length() ? (int)randomAccessFile.length() : (idxEndByte - idxBeginByte);
            byte[] bytes = new byte[size];
            randomAccessFile.seek(idxBeginByte);
            randomAccessFile.read(bytes);
            randomAccessFile.close();
            String textRead = new String(bytes, charsetName);
            return Result.getResult(true, textRead, String.format("The random text read from file [%s] is [%s].", filePath, textRead));
        } catch (FileNotFoundException e) {
            return Result.getResult(false, null, String.format("Failed to read random text from file [%s], it doesn't exist.", filePath));
        } catch (IOException e) {
            return Result.getResult(false, null, String.format("Failed to read random text from file [%s], more info = [%s].", filePath, e.getMessage()));
        }
    }

    /**
     * read text by index range of bytes using by UTF-8
     * @param filePath the file path
     * @param idxBeginByte the finger point of begin byte index, the first char's index is [1]
     * @param idxEndByte the finger point of end byte index, this must be large to the parameter [idxBeginByte]
     * @return Result.data is a String type
     */
    public static Result readTextUtf8(String filePath, int idxBeginByte, int idxEndByte){
        return readText(filePath, idxBeginByte, idxEndByte, "UTF8");
    }

    /**
     * read text by index range of bytes using by GBK
     * @param filePath the file path
     * @param idxBeginByte the finger point of begin byte index, the first char's index is [1]
     * @param idxEndByte the finger point of end byte index, this must be large to the parameter [idxBeginByte]
     * @return Result.data is a String type
     */
    public static Result readTextGbk(String filePath, int idxBeginByte, int idxEndByte){
        return readText(filePath, idxBeginByte, idxEndByte, "GBK");
    }

    /**
     * read text
     * @param filePath the file path
     * @param beginLineNumber the text start line number(include), the first line number is [1], value [-1] means no limits
     * @param endLineNumber the text end line number(include), value [-1] means no limits
     * @return Result.data is a String type
     */
    public static Result readTextLines(String filePath, int beginLineNumber, int endLineNumber){
        if(beginLineNumber < 1 && beginLineNumber != -1){
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], the begin line number [%s] must be bigger than [0], value [-1] means no limits.", filePath, beginLineNumber));
        }
        if(endLineNumber < 1 && endLineNumber != -1){
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], the end line number [%s] must be bigger than [0], value [-1] means no limits.", filePath, beginLineNumber));
        }
        if(endLineNumber != -1 && beginLineNumber > endLineNumber ){
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], the begin line number [%s] must be less than the end line number [%s].", filePath, beginLineNumber, endLineNumber));
        }

        // do read
        int lineNumber = 1;
        StringBuilder stringBuilder = new StringBuilder();
        try {
            InputStream inputStream = new FileInputStream(filePath);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String textLine;
            while((textLine = bufferedReader.readLine()) != null){
                if(lineNumber < beginLineNumber){
                    lineNumber++;
                    continue;
                }

                stringBuilder.append(textLine + "\n");
                lineNumber++;
                if(endLineNumber != -1 && lineNumber > endLineNumber){
                    break;
                }
            }
            inputStream.close();

            return Result.getResult(true, stringBuilder.toString(), String.format("Finished to read text from file [%s].", filePath));
        } catch (FileNotFoundException e) {
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], it doesn't exist.", filePath));
        } catch (IOException e) {
            return Result.getResult(false, null, String.format("Failed to read text from file [%s], more info = [%s].", filePath, e.getMessage()));
        }
    }

    /**
     * read text
     * @param filePath the file path
     * @param beginLineNumber the text start line number(include), the first line number is [1], value [-1] means no limits
     * @param endLineNumber the text end line number(include), value [-1] means no limits
     * @return Result.data is a String type
     */
    public static Result readText(String filePath, int beginLineNumber, int endLineNumber){
        return readTextLines(filePath, beginLineNumber, endLineNumber);
    }

    /**
     * write text
     * @param filePath the file path
     * @param text the text to be wrote
     * @param overwrite overwrite text file if it already exists
     * @return
     */
    public static Result writeText(String filePath, String text, boolean overwrite){
        File file = new File(filePath);
        boolean fileExists = file.exists();
        if(fileExists == true){
            if(overwrite == true){
                if(file.delete() == false){
                    return Result.getResult(false, null, String.format("Failed to write text to file [%s], failed to overwrite it, check the permission of the file firstly.", filePath));
                }
            }
            else {
                return Result.getResult(false, null, String.format("Failed to write text to file [%s], the file already exists, and it's set to be not overwrote.", filePath));
            }
        }

        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));
            bufferedWriter.write(text);
            bufferedWriter.close();
            if(fileExists == true){
                return Result.getResult(true, null, String.format("Overwrote text to file [%s].", filePath));
            }
            else {
                return Result.getResult(true, null, String.format("Wrote text to file [%s].", filePath));
            }
        } catch (IOException e) {
            return Result.getResult(false, null, String.format("Failed to write text to file [%s], more info = [%s].", filePath, e.getMessage()));
        }
    }

    /**
     * write text
     * @param filePath the file path
     * @param text the text to be wrote
     * @return
     */
    public static Result writeText(String filePath, String text){
        return writeText(filePath, text, false);
    }

    public static Result appendText(String filePath, String text){
        File file = new File(filePath);
        if(!file.exists()){
            return Result.getResult(false, null, String.format("Failed to append text to file [%s], it doesn't exist.", filePath));
        }

        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath, true));
            // take care, BufferedWriter.append() doesn't mean append, it's similar to BufferedWriter.write()
            bufferedWriter.append(text);
            bufferedWriter.close();

            return Result.getResult(true, null, String.format("Appended text to file [%s].", filePath));
        } catch (IOException e) {
            return Result.getResult(false, null, String.format("Failed to write text to file [%s], more info = [%s].", filePath, e.getMessage()));
        }
    }
}
