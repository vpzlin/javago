import org.vpzlin.javago.utils.FileUtil;

import java.util.Arrays;
import java.util.List;

public class TestFile {
    public static void test01(){
        String path = "C:\\GreenPark\\UserFile\\Desktop\\project\\test\\1.txt";
        String pathDir = "C:\\GreenPark\\UserFile\\Desktop\\project\\test\\testDir";
        String charsetName = "GBK";

//        System.out.println(FileUtil.readText(path, 0, 100, charsetName).getMessage());
//        System.out.println(FileUtil.touch(pathDir).getMessage());
        System.out.println(FileUtil.readTextUtf8(path, 1, 2000).getMessage());
    }

    public static void main(String[] args){
        test01();
    }
}

