package transaction;

import com.microdb.bufferpool.BufferPool;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.HeapTableFile;
import com.microdb.model.dbfile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.page.Page;
import com.microdb.model.page.heap.HeapPageID;
import com.microdb.operator.Delete;
import com.microdb.operator.SeqScan;
import com.microdb.transaction.Transaction;
import com.microdb.transaction.TransactionID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * 锁
 *
 * @author zhangjw
 * @version 1.0
 */
public class LockTest {
    public DataBase dataBase;
    private TableDesc personTableDesc;
    private BufferPool bufferPool;

    @Before
    public void initDataBase() throws IOException {
        DataBase dataBase = DataBase.getInstance();
        bufferPool = DataBase.getBufferPool();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();

        // 表中有两个int类型字段，person_id 字段为索引字段
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("person_id", FieldType.INT),
                new TableDesc.Attribute("person_age", FieldType.INT));

        File file = new File(fileName);
        file.deleteOnExit();
        TableDesc tableDesc = new TableDesc(attributes);
        TableFile tableFile = new HeapTableFile(file, tableDesc);
        dataBase.addTable(tableFile, "t_person");
        personTableDesc = tableDesc;
        this.dataBase = dataBase;
        // 表中初始化数据
        int num = 5;

        Transaction transaction = new Transaction();
        Connection.passingTransaction(transaction.getTransactionId());
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }
        transaction.commit();
    }

    @Test
    public void testInsertAndDeleteWithTransaction() throws IOException {
        Transaction transaction = new Transaction();
        Connection.passingTransaction(transaction.getTransactionId());
        TableFile tableFile = dataBase.getDbTableByName("t_person").getTableFile();

        int num = 5;
        long l1 = System.currentTimeMillis();
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }

        System.out.println("开始打印表数据====");

        SeqScan scan = new SeqScan(tableFile.getTableId());

        scan.open();
        while (scan.hasNext()) {
            System.out.println(scan.next());
        }

        // 通过Delete操作符删除，执行非常快速
        //
        Delete delete = new Delete(scan);
        delete.loopDelete();

        scan.open();
        Assert.assertFalse(scan.hasNext());
        System.out.println("耗时ms:" + (System.currentTimeMillis() - l1));

    }

    @Test
    public void testTransactionCommit() throws IOException {
        Transaction transaction = new Transaction();
        Connection.passingTransaction(transaction.getTransactionId());
        TableFile tableFile = dataBase.getDbTableByName("t_person").getTableFile();
        int num = 30000;
        long l1 = System.currentTimeMillis();
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }

        System.out.println("开始打印表数据====");
        SeqScan scan = new SeqScan(tableFile.getTableId());
        scan.open();
        while (scan.hasNext()) {
            System.out.println(scan.next());
        }
        System.out.println("耗时ms:" + (System.currentTimeMillis() - l1));


        // 添加不经过缓存的读取文件 用于测试

        scan.open();
        Assert.assertFalse(scan.hasNext());


    }

    /**
     * 并发获取同一个页x锁
     */
    @Test
    public void concurrentGetSamePageXLock() throws ExecutionException, InterruptedException, TimeoutException, IOException {
        System.out.println("concurrentGetSamePageXLock----------------start");
        DbTable table = DataBase.getInstance().getDbTableByName("t_person");
        ExecutorService threadPool = Executors.newCachedThreadPool();
        int threadNum = 2;
        HeapPageID heapPageID = new HeapPageID(table.getTableId(), 0);

        ArrayList<Future<Page>> pageResult = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            TransactionID transactionId = new Transaction().getTransactionId();
            Future<Page> pageFuture = threadPool.submit(() -> {
                Connection.passingTransaction(transactionId);
                return DataBase.getBufferPool().getPage(heapPageID);
            });
            pageResult.add(pageFuture);

            // 使线程执行有先先后顺序
            Thread.sleep(200);
        }

        // 线程1获取锁成功
        Page page = pageResult.get(0).get();
        assertNotNull(page);

        // 线程2获取锁失败
        try {
            pageResult.get(1).get(1,TimeUnit.SECONDS);
            fail("expected timeout");
        } catch (Exception ignored) {
        }

        // 终止所有线程
        threadPool.shutdownNow();
        System.out.println("concurrentGetSamePageXLock----------------end");
    }


}
