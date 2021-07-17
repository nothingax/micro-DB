package integrated.operator;

import base.TestBase;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.row.Row;
import com.microdb.model.table.DbTable;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.HeapTableFile;
import com.microdb.model.table.tablefile.TableFile;
import com.microdb.operator.Aggregate;
import com.microdb.operator.AggregateType;
import com.microdb.operator.SeqScan;
import com.microdb.transaction.Lock;
import com.microdb.transaction.Transaction;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * 聚合操作符Test
 *
 * @author zhangjw
 * @version 1.0
 */
public class AggregateTest extends TestBase {

    public DataBase dataBase;

    @Before
    public void initDataBase() throws IOException {
        DataBase dataBase = DataBase.getInstance();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();
        // 定义表结构，2个int字段
        List<TableDesc.Attribute> attributes =
                Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT),
                        new TableDesc.Attribute("f2", FieldType.INT));
        TableDesc tableDesc = new TableDesc(attributes);
        File file = new File(fileName);
        file.deleteOnExit();
        TableFile tableFile = new HeapTableFile(file, tableDesc);

        // tableDesc
        dataBase.addTable(tableFile, "t_person", tableDesc);

        this.dataBase = dataBase;


        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Row row = new Row(tablePerson.getTableDesc());


        // f2是group by字段, f1是被聚合字段
        // 插入以下数据
        for (int i = 0; i < 3; i++) {
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(100));
            tablePerson.insertRow(row);
        }

        for (int i = 0; i < 5; i++) {
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(200));
            tablePerson.insertRow(row);
        }

        for (int i = 0; i < 10; i++) {
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(300));
            tablePerson.insertRow(row);
        }

        transaction.commit();
    }

    /**
     * 基于seqScan和Aggregate实现的聚合查询
     * sql_1：select max(f1) from t_person group by f2
     * sql_2：select count(f1) from t_person group by f2
     */
    @Test
    public void testAggregate() throws IOException {
        SeqScan seqScan = new SeqScan(this.dataBase.getDbTableByName("t_person").getTableId());
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        // select max(f1) from t_person group by f2
        AggregateType max = AggregateType.MAX;
        this.doAggregateAndPrint(seqScan, max);

        // select count(f1) from t_person group by f2
        this.doAggregateAndPrint(seqScan, AggregateType.COUNT);

        transaction.commit();
    }

    /**
     * 聚合并打印结果
     *
     * @param seqScan       提供数据的操作符
     * @param aggregateType 聚合方式
     */
    private void doAggregateAndPrint(SeqScan seqScan, AggregateType aggregateType) {
        Aggregate aggregate = new Aggregate(seqScan, 0, 1, aggregateType);
        aggregate.open();
        while (aggregate.hasNext()) {
            Row next = aggregate.next();
            System.out.println(next.toString());
        }
        aggregate.close();
    }
}
