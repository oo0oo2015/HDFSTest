package wang.oo0oo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        System.out.println("--------课程实验--------");
        System.out.println("【4】实验四：文件基本操作\n" +
                "【5】实验五：WordCount程序\n" +
                "【6】实验六：HBase测试示例程序");
        System.out.print("请选择要执行的实验：");
        int op = sc.nextInt();
        switch (op) {
            case 4:
                BasicFileOperation.main();
                break;
            case 5:
                WordCount.WordCountDriver.main();
                break;
            case 6:
                HBaseTest.main();
            default:

        }
    }
}
