import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;

public class TestHdfsOperation {
    @Test
    public void testMkdir() throws IOException {
        HdfsOperation.mkdir("/hdfs/practice");
    }

    @Test
    public void testUploadFile() throws IOException {
        HdfsOperation.uploadFile("/Users/zhenqi/Documents/LecturesNotes/开课吧-大数据/09-Hadoop-1-HDFS/课后资料/课堂笔记.txt",
                "/hdfs/practice/lectureNote.txt");
    }

    @Test
    public void testDownloadFile() throws IOException {
        HdfsOperation.downloadFile("/hdfs/practice/lectureNote.txt",
                "/Users/zhenqi/IdeaProjects/BigData/operationFiles/");
    }

    @Test
    public void testDeleteFile() throws IOException {
        HdfsOperation.deleteFile("/hdfs/practice/lectureNotes");
    }

    @Test
    public void testRenameFile() throws IOException {
        HdfsOperation.renameFile("/hdfs/practice/lectureNote.txt",
                "/hdfs/practice/lectureNotes");
    }

    @Test
    public void testListFiles() throws IOException, URISyntaxException {
        HdfsOperation.listFiles("/hdfs");
    }
}
