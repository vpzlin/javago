import org.vpzlin.javago.utils.FileUtil;

public class TestFile {
    public static void test01(){
        String path = "C:\\GreenPark\\UserFile\\Desktop\\project\\test\\1.tx1t";
        System.out.println(FileUtil.canExecute(path));
        System.out.println(FileUtil.getInfoByCode(0));
    }

    public static void main(String[] args){
        test01();
    }
}

