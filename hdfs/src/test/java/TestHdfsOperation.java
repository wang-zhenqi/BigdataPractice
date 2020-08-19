import org.testng.annotations.Test;

import java.io.IOException;

public class TestHdfsOperation {
    @Test
    public void testMkdir() throws IOException {
        HdfsOperation.mkdir("/hdfs/practice");
    }

    @Test
    public void testUploadFile() throws IOException {
        HdfsOperation.uploadFile("/Users/zhenqi/Documents/LecturesNotes/开课吧-大数据/09-Hadoop-1-HDFS/课后资料",
                "/hdfs/practice/lectureNote.txt");
    }
}
