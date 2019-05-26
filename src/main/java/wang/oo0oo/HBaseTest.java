package wang.oo0oo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ThreadPoolExecutor;

class HBaseTest {

    private static Configuration conf;
    private static Connection con;
    private static Admin adm;

    static void main() {

        int op = 0;
        while (op != 7) {
            op = menu();
            try {
                if (con == null) {
                    init();
                }

                switch (op) {
                    case 1:
                        System.out.println("*****创建表*****");
                        createTable("stu", new String[]{"info", "bigdata"});
                        System.out.println("*****创建完毕*****");
                        break;
                    case 2:
                        System.out.println("*****查看所有表*****");
                        listTables();
                        System.out.println("*****显示完毕*****");
                        break;
                    case 3:
                        System.out.println("*****插入数据*****");
                        insertRow("stu", "2016001", "info", "name", "mark");
                        insertRow("stu", "2016001", "info", "age", "22");
                        insertRow("stu", "2016001", "bigdata", "main", "85");
                        System.out.println("*****插入完毕*****");
                        break;
                    case 4:
                        System.out.println("*****删除数据*****");
                        deleteRow("stu", "2016001", "info", "age");
                        System.out.println("*****删除完毕*****");
                        break;
                    case 5:
                        System.out.println("*****根据rowKey查找数据*****");
                        getData("stu", "2016001", "info", "name");
                        System.out.println("*****查找完毕*****");
                        break;
                    case 6:
                        System.out.println("*****删除表*****");
                        deleteTable("stu");
                        System.out.println("*****删除完毕*****");
                        break;
                    default:
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        close();
    }

    private static int menu() {
        System.out.println("====HBase测试示例程序====");
        System.out.println("【1】创建表\n" +
                "【2】查看所有表\n" +
                "【3】插入数据\n" +
                "【4】删除数据\n" +
                "【5】根据rowKey查找数据\n" +
                "【6】删除表\n" +
                "【7】退出");
        System.out.print("请选择要执行的操作：");
        Scanner sc = new Scanner(System.in);
        return sc.nextInt();
    }

    //建立连接
    private static void init() throws IOException {
        conf = HBaseConfiguration.create();     //对 HBase 进行配置
        conf.set("hbase.rootdir", "hdfs://bigdata001:9000/hbase");

        con = ConnectionFactory.createConnection(conf);
        adm = con.getAdmin();     //管理 HBase数据库的表信息
    }

    //创建表
    private static void createTable(String myTableName, String[] colFamily) throws IOException {

        TableName tableName = TableName.valueOf(myTableName);      //管理表名
        if (adm.tableExists(tableName)) {
            System.out.println("table is exists!");
        } else {
            HTableDescriptor htd = new HTableDescriptor(tableName);     //包含表名及其列族
            for (String str : colFamily) {
                HColumnDescriptor hcd = new HColumnDescriptor(str);     //维护列族信息
                htd.addFamily(hcd);
            }
            adm.createTable(htd);
        }

    }

    //关闭连接
    private static void close() {
        try {
            if (adm != null) {
                adm.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //删除表
    private static void deleteTable(String myTableName) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (adm.tableExists(tableName)) {   //管理数据库的表信息
            adm.disableTable(tableName);
            adm.deleteTable(tableName);
        }
    }

    //查看所有表
    private static void listTables() throws IOException {
        HTableDescriptor[] htds = adm.listTables();     //表的名字及其对应表的列族
        for (HTableDescriptor htd : htds) {
            System.out.println(htd.getNameAsString());
        }
    }

    //插入一行
    private static void insertRow(String myTableName, String rowKey, String colFamily, String col, String val) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        ThreadPoolExecutor t = HTable.getDefaultExecutor(conf);

        HTable table = new HTable(conf, tableName);     //和 HBase 表直接通信
        Put put = new Put(rowKey.getBytes());       //对单个行执行添加操作
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }

    //删除一行
    private static void deleteRow(String myTableName, String rowKey, String colFamily, String col) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        HTable table = new HTable(conf, tableName);
        Delete delete = new Delete(rowKey.getBytes());    //对单个行执行删除操作
        delete.addFamily(Bytes.toBytes(colFamily));
        delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        table.delete(delete);
        table.close();
    }

    //根据rowKey查找数据
    private static void getData(String myTableName, String rowKey, String colFamily, String col) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());    //获取单个行的相关信息
        Result result = table.get(get);   //存储 Get 或者 Scan 操作后获取表的单行值
        showCell(result);
        table.close();
    }

    //格式化输出
    private static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timestamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }
}
