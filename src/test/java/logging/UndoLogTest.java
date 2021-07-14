package logging;

import com.microdb.bufferpool.BufferPool;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.HeapTableFile;
import com.microdb.model.table.tablefile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.operator.SeqScan;
import com.microdb.transaction.Lock;
import com.microdb.transaction.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * UndoLogTest
 *
 * @author zhangjw
 * @version 1.0
 */
public class UndoLogTest {

    public DataBase dataBase;
    private TableDesc personTableDesc;
    private BufferPool bufferPool;
    private int tableId;

    @Before
    public void initDataBase() throws IOException {
        DataBase dataBase = DataBase.getInstance();
        bufferPool = dataBase.getBufferPool();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();

        // 表中有两个int类型字段，person_id 字段为索引字段
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("person_id", FieldType.INT),
                new TableDesc.Attribute("person_age", FieldType.INT));

        File file = new File(fileName);
        file.deleteOnExit();
        TableDesc tableDesc = new TableDesc(attributes);
        TableFile tableFile = new HeapTableFile(dataBase,file, tableDesc);
        dataBase.addTable(tableFile, "t_person");
        this.tableId = tableFile.getTableId();
        personTableDesc = tableDesc;
        this.dataBase = dataBase;
    }


    /**
     * 测试基于undoLog的回滚
     * 模拟Steal策略：事务未提交，但缓冲区的脏页被刷盘
     * 事务回滚，需要从undoLog中取回原始数据恢复脏页
     */
    @Test
    public void testRollBack() throws IOException {
        // 表中初始化数据
        int num = 10;

        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            dataBase.getBufferPool().insertRow(row, "t_person");
        }


        // 模拟Steal：事务未提交，但缓冲区的脏页被刷盘
        dataBase.getBufferPool().flushAllPage();
        // 事务终止
        transaction.abort();

        // 再次读取表，数据应不存在
        Transaction tx2 = new Transaction(Lock.LockType.SLock);
        tx2.start();
        Connection.passingTransaction(tx2);
        SeqScan scan = new SeqScan(this.tableId);
        scan.open();
        Assert.assertFalse(scan.hasNext());
        tx2.commit();
    }

}
