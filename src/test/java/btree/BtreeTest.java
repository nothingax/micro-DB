package btree;

import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.BTreeFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
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
public class BtreeTest {
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
    public void testInsert() throws IOException {
        DbTable t_person = dataBase.getDbTableByName("t_person");

        for (int i = 0; i < 100; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            t_person.insertRow(row);
        }
    }



}
