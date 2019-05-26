package wang.oo0oo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Scanner;

class BasicFileOperation {
    private static FileSystem client;
    private static InputStream input;
    private static OutputStream output;

    private static void HDFSMkdir() throws IOException {
        if (client.mkdirs(new Path("/bbdir"))) {
            System.out.println("创建成功！");
        } else {
            System.out.println("创建失败！");
        }
    }

    private static void HDFSUpload() throws IOException {
        //创建本地文件的输入流
        input = new FileInputStream("/root/bb.txt");
        //创建HDFS的输出流
        output = client.create(new Path("/bbdir/bbout.txt"));
        //写文件到HDFS
        byte[] buffer = new byte[1024];
        int len = 0;
        while ((len = input.read(buffer)) != -1) {
            output.write(buffer, 0, len);
        }
        //防止输出数据不完整
        output.flush();
        //使用工具类IOUtils上传或下载
        //IOUtils.copy(input, output);
        //关闭输入输出流
        input.close();
        output.close();
    }

    private static void HDFSDownload() throws IOException {
        //创建本地文件的输出流
        output = new FileOutputStream("/root/bbout.txt");
        //创建HDFS的输入流
        input = client.open(new Path("/bbdir/bbout.txt"));
        //写文件到HDFS
        byte[] buffer = new byte[1024];
        int len = 0;
        while ((len = input.read(buffer)) != -1) {
            output.write(buffer, 0, len);
        }
        //防止输出数据不完整
        output.flush();
        //使用工具类IOUtils上传或下载
        //IOUtils.copy(input, output);
        //关闭输入输出流
        input.close();
        output.close();
    }

    private static void HDFSFileIfExist() throws IOException {
        //声明文件对象
        String fileName = "/bbdir/bbout.txt";
        //判断文件是否存在
        if (client.exists(new Path(fileName))) {
            System.out.println("文件存在！");
        } else {
            System.out.println("文件不存在！");
        }
    }

    static void main() throws IOException {
        //设置root权限
        System.setProperty("HADOOP_USER_NAME", "root");
        //创建HDFS连接对象client
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata001:9000");
        client = FileSystem.get(conf);

        System.out.println("====HDFS测试示例程序====");
        System.out.println("请选择操作：\n" +
                "1.在HDFS根目录创建文件夹/bbdir\n" +
                "2.上传文件/root/bb.txt到HDFS的/bbdir/aaout.txt\n" +
                "3.下载HDFS的/bbdir/bbout.txt文件到/root/bbout.txt\n" +
                "4.检测HDFS的文件/bbdir/bbout.txt是否存在\n");
        Scanner sc = new Scanner(System.in);
        int op = sc.nextInt();
        switch (op) {
            case 1:
                HDFSMkdir();
                break;
            case 2:
                HDFSUpload();
                break;
            case 3:
                HDFSDownload();
                break;
            case 4:
                HDFSFileIfExist();
                break;
            default:
        }


        //关闭连接对象
        client.close();

    }
}
