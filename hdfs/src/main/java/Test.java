import java.io.IOException;

public class Test {
    public static void main(String[] args) {
        try {
            HdfsOperation.mkdir("/hdfs/practice");
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
}
