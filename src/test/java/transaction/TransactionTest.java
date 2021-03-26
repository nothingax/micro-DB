package transaction;

import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.BTreeFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.operator.btree.BtreeScan;
import com.microdb.transaction.Transaction;
import com.microdb.transaction.TransactionID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * 事务Test
 *
 * @author zhangjw
 * @version 1.0
 */
public class TransactionTest {

    public DataBase dataBase;

    private TableDesc personTableDesc;

    @Before
    public void initDataBase() {
        DataBase dataBase = DataBase.getInstance();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();

        // 表中有两个int类型字段，person_id 字段为索引字段
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("person_id", FieldType.INT),
                new TableDesc.Attribute("person_age", FieldType.INT));

        File file = new File(fileName);
        file.deleteOnExit();
        TableDesc tableDesc = new TableDesc(attributes);
        BTreeFile bTreeFile = new BTreeFile(file, tableDesc, 0);
        dataBase.addTable(bTreeFile, "t_person");
        this.dataBase = dataBase;

        personTableDesc = tableDesc;
    }


    @Test
    public void test() throws IOException {

        Transaction transaction = new Transaction();
        TransactionID transactionId = transaction.getTransactionId();
        Connection.passingTransaction(transactionId);


        BTreeFile tableFile = (BTreeFile) dataBase.getDbTableByName("t_person").getTableFile();
        int tableId = tableFile.getTableId();
        int num = 200;
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }

        transaction.commit();


        System.out.println("开始打印表数据====");
        // printTree(tableFile, tableId);

        BtreeScan scan = new BtreeScan(tableFile.getTableId(), null);

        System.out.println("开始打印====");
        // 删除并打印
        // for (int i = 1; i <= num; i++) {
        //     deleteOne(tableFile, scan);
        //     printTree(tableFile, tableId);
        // }

        scan.open();
        Assert.assertTrue(scan.hasNext());

    }

    // 关键点测试

    // 创建事务，id自增

    // 锁在threadLocal中的传递，threadLocal中线程复用的影响

    // 获取锁

    // 事务提交前，脏页不能落盘
    // 事务提交后数据需要落盘

    // 事务结束后，锁释放成功

    // 锁的正确性验证，事务的锁

    // TODO 锁的分类

}
