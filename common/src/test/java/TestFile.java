import org.vpzlin.javago.utils.FileUtil;

import java.util.Arrays;
import java.util.List;

public class TestFile {
    public static void test01(){
        String path = "C:\\GreenPark\\UserFile\\Desktop\\project\\test\\1.txt";

//        System.out.println(FileUtil.touch(path).getMessage());
        List<String> list = Arrays.asList(null);
        System.out.println(list);
    }

    public static void main(String[] args){
        test01();
    }
}

