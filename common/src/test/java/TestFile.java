import org.vpzlin.javago.utils.FileUtil;

import java.util.Arrays;
import java.util.List;

public class TestFile {
    public static void test01(){
        String filePath = "C:\\GreenPark\\UserFile\\Desktop\\project\\test\\1.txt";
        String pathDir = "C:\\GreenPark\\UserFile\\Desktop\\project\\test\\testDir";
        String charsetName = "GBK";

//        System.out.println(FileUtil.readText(filePath, 0, 100, charsetName).getMessage());
//        System.out.println(FileUtil.touch(pathDir).getMessage());
//        System.out.println(FileUtil.readTextUtf8(filePath, 1, 2000).getMessage());
//        System.out.println(FileUtil.readTextLines(filePath, 2, 3).getData());
//        System.out.println(FileUtil.writeText(filePath, "aaa", true).getMessage());
        System.out.println(FileUtil.appendText(filePath, "aaa").getMessage());
    }

    public static void main(String[] args){
        test01();
    }
}

