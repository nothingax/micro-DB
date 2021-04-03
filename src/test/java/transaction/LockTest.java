package transaction;

import com.microdb.bufferpool.BufferPool;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.table.DbTable;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.HeapTableFile;
import com.microdb.model.table.tablefile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.page.Page;
import com.microdb.model.page.heap.HeapPageID;
import com.microdb.transaction.Lock;
import com.microdb.transaction.Transaction;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

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
        int num = 100;

        Transaction transaction = new Transaction(Lock.LockType.XLock);
        Connection.passingTransaction(transaction);
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }
        transaction.commit();
    }

    /**
     * 并发获取同一个页x锁，应只有一个事务成功
     */
    @Test
    public void concurrentGetXLockSamePage() throws ExecutionException, InterruptedException {
        System.out.println("concurrentGetSamePageXLock----------------start");
        DbTable table = DataBase.getInstance().getDbTableByName("t_person");
        ExecutorService threadPool = Executors.newCachedThreadPool();
        int threadNum = 2;
        // 同页
        HeapPageID heapPageID = new HeapPageID(table.getTableId(), 0);

        ArrayList<Future<Page>> futureResult = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            Transaction transaction = new Transaction(Lock.LockType.XLock);
            Future<Page> pageFuture = threadPool.submit(() -> {
                Connection.passingTransaction(transaction);
                return DataBase.getBufferPool().getPage(heapPageID);
            });
            futureResult.add(pageFuture);

            // 使线程执行有先先后顺序
            Thread.sleep(200);
        }

        // 线程1获取锁成功
        Page page = futureResult.get(0).get();
        assertNotNull(page);

        // 线程2获取锁失败
        try {
            futureResult.get(1).get(1, TimeUnit.SECONDS);
            fail("expected timeout");
        } catch (Exception ignored) {
        }

        // 终止所有线程
        threadPool.shutdownNow();
        System.out.println("concurrentGetSamePageXLock----------------end");
    }

    /**
     * 多个事务获取不同页的x锁，应互不阻塞，均成功
     */
    @Test
    public void concurrentGetXLockOnDifferentPages() throws ExecutionException, InterruptedException {
        DbTable table = DataBase.getInstance().getDbTableByName("t_person");
        ExecutorService threadPool = Executors.newCachedThreadPool();
        int threadNum = 2;

        ArrayList<Future<Page>> futureResult = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            // 不同页
            HeapPageID heapPageID = new HeapPageID(table.getTableId(), i);
            Transaction transaction = new Transaction(Lock.LockType.XLock);
            Future<Page> pageFuture = threadPool.submit(() -> {
                Connection.passingTransaction(transaction);
                return DataBase.getBufferPool().getPage(heapPageID);
            });
            futureResult.add(pageFuture);
        }

        // 线程1获取锁成功
        Page page = futureResult.get(0).get();
        assertNotNull(page);

        // 线程2获取锁成功
        Page page2 = futureResult.get(1).get();
        assertNotNull(page2);

        // 终止所有线程
        threadPool.shutdownNow();
    }

    /**
     * 并发获取同一个页S锁，应互不阻塞，均成功
     */
    @Test
    public void concurrentGetSLockSamePage() throws ExecutionException, InterruptedException {
        DbTable table = DataBase.getInstance().getDbTableByName("t_person");
        ExecutorService threadPool = Executors.newCachedThreadPool();
        int threadNum = 2;
        // 同一页
        HeapPageID heapPageID = new HeapPageID(table.getTableId(), 0);

        ArrayList<Future<Page>> futureResult = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            Transaction transaction = new Transaction(Lock.LockType.SLock);
            Future<Page> pageFuture = threadPool.submit(() -> {
                Connection.passingTransaction(transaction);
                return DataBase.getBufferPool().getPage(heapPageID);
            });
            futureResult.add(pageFuture);
        }

        // 线程1获取锁成功
        Page page = futureResult.get(0).get();
        assertNotNull(page);

        // 线程2获取锁成功
        Page page2 = futureResult.get(1).get();
        assertNotNull(page2);

        // 终止所有线程
        threadPool.shutdownNow();
    }

    /**
     * 并发获取不同页S锁，应互不阻塞，均成功
     */
    @Test
    public void concurrentGetSLockOnDifferentPage() throws ExecutionException, InterruptedException {
        DbTable table = DataBase.getInstance().getDbTableByName("t_person");
        ExecutorService threadPool = Executors.newCachedThreadPool();
        int threadNum = 2;
        ArrayList<Future<Page>> futureResult = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            // 不同页
            HeapPageID heapPageID = new HeapPageID(table.getTableId(), i);
            Transaction transaction = new Transaction(Lock.LockType.SLock);
            Future<Page> pageFuture = threadPool.submit(() -> {
                Connection.passingTransaction(transaction);
                return DataBase.getBufferPool().getPage(heapPageID);
            });
            futureResult.add(pageFuture);
        }

        // 线程1获取锁成功
        Page page = futureResult.get(0).get();
        assertNotNull(page);

        // 线程2获取锁成功
        Page page2 = futureResult.get(1).get();
        assertNotNull(page2);

        // 终止所有线程
        threadPool.shutdownNow();
    }

    // 锁升级：一个事务先后获取一个页s锁、x锁 ，锁应升级为x锁
    @Test
    public void getSAndXLockOnSamePage() throws ExecutionException, InterruptedException {
        DbTable table = DataBase.getInstance().getDbTableByName("t_person");
        ExecutorService threadPool = Executors.newCachedThreadPool();
        int threadNum = 2;
        ArrayList<Future<Page>> futureResult = new ArrayList<>();
        HeapPageID heapPageID = new HeapPageID(table.getTableId(), 0);

        Transaction transaction = new Transaction(Lock.LockType.SLock);
        Future<Page> pageFuture = threadPool.submit(() -> {
            Connection.passingTransaction(transaction);
            return DataBase.getBufferPool().getPage(heapPageID);
        });
        futureResult.add(pageFuture);

        transaction.setLockType(Lock.LockType.XLock);
        Future<Page> pageFuture2 = threadPool.submit(() -> {
            Connection.passingTransaction(transaction);
            return DataBase.getBufferPool().getPage(heapPageID);
        });
        futureResult.add(pageFuture2);

        Thread.sleep(100);
        // 锁应升级为x锁,新的事务获取应获取不到
        Transaction transaction3 = new Transaction(Lock.LockType.SLock);
        Future<Page> pageFuture3 = threadPool.submit(() -> {
            Connection.passingTransaction(transaction3);
            return DataBase.getBufferPool().getPage(heapPageID);
        });
        futureResult.add(pageFuture3);

        // 1获取锁成功
        Page page = futureResult.get(0).get();
        assertNotNull(page);

        // 2获取锁成功
        Page page2 = futureResult.get(1).get();
        assertNotNull(page2);

        // 线程3获取锁失败
        try {
            futureResult.get(2).get(1, TimeUnit.SECONDS);
            fail("expected timeout");
        } catch (Exception ignored) {
        }


        // 终止所有线程
        threadPool.shutdownNow();
    }


    // 锁升级：一个事务先后获取一个页x锁、s锁 ，应保持x锁不变
    @Test
    public void getXAndSLockOnSamePage() throws ExecutionException, InterruptedException {
        DbTable table = DataBase.getInstance().getDbTableByName("t_person");
        ExecutorService threadPool = Executors.newCachedThreadPool();
        int threadNum = 2;
        ArrayList<Future<Page>> futureResult = new ArrayList<>();
        HeapPageID heapPageID = new HeapPageID(table.getTableId(), 0);

        Transaction transaction = new Transaction(Lock.LockType.XLock);
        Future<Page> pageFuture = threadPool.submit(() -> {
            Connection.passingTransaction(transaction);
            return DataBase.getBufferPool().getPage(heapPageID);
        });
        futureResult.add(pageFuture);


        Thread.sleep(100);
        transaction.setLockType(Lock.LockType.SLock);
        Future<Page> pageFuture2 = threadPool.submit(() -> {
            Connection.passingTransaction(transaction);
            return DataBase.getBufferPool().getPage(heapPageID);
        });
        futureResult.add(pageFuture2);

        Thread.sleep(100);
        // 锁应保持为x锁,新的事务获取应获取不到锁
        Transaction transaction3 = new Transaction(Lock.LockType.SLock);
        Future<Page> pageFuture3 = threadPool.submit(() -> {
            Connection.passingTransaction(transaction3);
            return DataBase.getBufferPool().getPage(heapPageID);
        });
        futureResult.add(pageFuture3);

        // 1获取锁成功
        Page page = futureResult.get(0).get();
        assertNotNull(page);

        // 2获取锁成功
        Page page2 = futureResult.get(1).get();
        assertNotNull(page2);

        // 线程3获取锁失败
        try {
            futureResult.get(2).get(1, TimeUnit.SECONDS);
            fail("expected timeout");
        } catch (Exception ignored) {
        }

        // 终止所有线程
        threadPool.shutdownNow();
    }
}
