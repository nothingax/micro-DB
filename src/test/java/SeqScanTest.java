import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.HeapTableFile;
import com.microdb.model.dbfile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
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
 * test
 *
 * @author zhangjw
 * @version 1.0
 */
public class SeqScanTest {

    public DataBase dataBase;

    @Before
    public void initDataBase() throws IOException {
        DataBase dataBase = DataBase.getInstance();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT));
        TableDesc tableDesc = new TableDesc(attributes);
        TableFile tableFile = new HeapTableFile(new File(fileName),tableDesc);

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
     * 基于seqScan 实现的简单查询，等同于sql：select * from t_person
     */
    @Test
    public void testSimpleQueryBasedOnSeqScan() throws IOException {
        SeqScan scan = new SeqScan(this.dataBase.getDbTableByName("t_person").getTableId());
        scan.open();
        while (scan.hasNext()) {
            Row next = scan.next();
            next.getFields().forEach(x-> System.out.println(x.toString()));
        }

        scan.close();
    }
}
