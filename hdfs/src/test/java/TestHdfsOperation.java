import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;

public class TestHdfsOperation {
    private final String DATA_ROOT = "/Users/zhenqi/IdeaProjects/BigData/data";

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
        HdfsOperation.downloadFile("/mr-data/grouping/outputs/top2",
                DATA_ROOT + "/mr/grouping/top2");
    }

    @Test
    public void testDeleteFile() throws IOException {
        HdfsOperation.deleteFile("/hdfs/practice/mergedFile_1");
    }

    @Test
    public void testRenameFile() throws IOException {
        HdfsOperation.renameFile("/hdfs/practice/lectureNote.txt",
                "/hdfs/practice/lectureNotes");
    }

    @Test
    public void testListFiles() throws IOException, URISyntaxException {
        HdfsOperation.listFiles("/mr-data/wordcount/");
    }

    @Test
    public void testPutFileToHDFS() throws IOException {
        HdfsOperation.putFileToHDFS("../operationFiles/lectureNote.txt", "/hdfs/practice/lectureNote.txt");
    }

    @Test
    public void testGetFileFromHDFS() throws IOException {
        HdfsOperation.getFileFromHDFS("/mr-data/wordcount/inputs/PrideAndPrejudice.txt", "../data/mr/wordcount/input/PrideAndPrejudice.txt");
    }

    @Test
    public void testMergeFile() throws IOException {
        HdfsOperation.mergeFile("/Users/zhenqi/IdeaProjects/BigData/data/littleFiles",
                "hdfs://knot1:8020/hdfs/practice/mergedFile_1");
    }
}
