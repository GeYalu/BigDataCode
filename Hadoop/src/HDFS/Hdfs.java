package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.*;
import java.net.URI;

public class Hdfs {


    public static void main(String[] args) throws Exception {
        try {

            getDirectoryFromHdfs();
            uploadToHdfs();
            //deleteFromHdfs();
            //getDirectoryFromHdfs();
            //appendToHdfs();
            readFromHdfs();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            System.out.println("SUCCESS");
        }
    }


    /*上传到HDFS*/
    private static void uploadToHdfs() throws IOException {
        String localSrc = "hdfs.txt";
        String dst = "hdfs://192.168.2.240:9000/home/hadoop/hdfs/geyalu_3.txt";
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst) {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }


    /*从HDFS下载*/
    private static void readFromHdfs() throws IOException {
        String dst = "hdfs://192.168.2.240:9000/home/hadoop/hdfs/geyalu_3.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(dst));

        OutputStream out = new FileOutputStream("geyalu_3.txt");
        byte[] ioBuffer = new byte[1024];
        int readLen = hdfsInStream.read(ioBuffer);

        while (-1 != readLen) {
            out.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        out.close();
        hdfsInStream.close();
        fs.close();
    }


    /**
     * append 用的很少
     */
    private static void appendToHdfs() throws FileNotFoundException, IOException {
        String dst = "hdfs://192.168.0.113:9000/user/zhangzk/qq.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FSDataOutputStream out = fs.append(new Path(dst));

        int readLen = "zhangzk add by hdfs Java api".getBytes().length;

        while (-1 != readLen) {
            out.write("zhangzk add by hdfs java api".getBytes(), 0, readLen);
        }
        out.close();
        fs.close();
    }


    /*删除hdfs上文件*/
    private static void deleteFromHdfs() throws IOException {
        String dst = "hdfs://192.168.2.240:9000/home/hadoop/hdfs/geyalu2.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        fs.deleteOnExit(new Path(dst));
        fs.close();
    }


    /**
     * 获取HDFS目录
     */
    private static void getDirectoryFromHdfs() throws FileNotFoundException, IOException {
        String dst = "hdfs://192.168.2.240:9000/home/hadoop/hdfs";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FileStatus fileList[] = fs.listStatus(new Path(dst));
        int size = fileList.length;
        for (int i = 0; i < size; i++) {
            System.out.println("name:" + fileList[i].getPath().getName() + "/t/tsize:" + fileList[i].getLen());
        }
        fs.close();
    }

}