import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.operator.Filter;
import com.microdb.operator.Predicate;
import com.microdb.operator.SeqScan;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * TODO
 *
 * @author zhangjw
 * @version 1.0
 */
public class FilterTest {
    public DataBase dataBase;
    @Before
    public void initDataBase() throws IOException {
        DataBase dataBase = DataBase.getInstance();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();
        TableFile tableFile = new TableFile(new File(fileName));
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT));
        TableDesc tableDesc = new TableDesc(attributes);
        // tableDesc
        dataBase.addTable(tableFile, "t_person", tableDesc);

        this.dataBase = dataBase;

        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Row row = new Row(tablePerson.getTableDesc());

        // 第1页
        for (int i = 0; i < 20; i++) {
            row.setField(0, new IntField(i));
            tablePerson.insertRow(row);
            int existPageCount = tablePerson.getTableFile().getExistPageCount();
            Assert.assertEquals(1, existPageCount);
        }
    }

    /**
     * 基于seqScan和filter实现的条件查询
     * sql_1：select * from t_person where f1=5
     * sql_2：select * from t_person where f1<5
     */
    @Test
    public void testSimpleQueryBasedOnSeqScan() throws IOException {
        SeqScan seqScan = new SeqScan(this.dataBase.getDbTableByName("t_person").getTableId());
        System.out.println("过滤第0字段等于'5'的行");
        Predicate predicateEQ = new Predicate(0, Predicate.OperationEnum.EQUALS, new IntField(5));
        Filter filter = new Filter(predicateEQ, seqScan);
        filter.open();
        while (filter.hasNext()) {
            Row next = filter.next();
            next.getFields().forEach(x-> System.out.println(x.toString()));
        }
        filter.close();


        System.out.println("过滤第0字段<'5'的行");
        Predicate predicateLessThan = new Predicate(0, Predicate.OperationEnum.LESS_THAN, new IntField(5));
        Filter filter2 = new Filter(predicateLessThan, seqScan);
        filter2.open();
        while (filter2.hasNext()) {
            Row next = filter2.next();
            next.getFields().forEach(x-> System.out.println(x.toString()));
        }
        filter2.close();
    }
}
