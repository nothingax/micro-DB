import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.TableDesc;
import com.microdb.model.Tuple;
import com.microdb.model.dbfile.DbTableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * test
 *
 * @author zhangjw
 * @version 1.0
 */
public class DataBaseTest {

    public DataBase dataBase;

    @Before
    public void initDataBase() {
        DataBase dataBase = new DataBase();
        // 创建数据库文件
        DbTableFile dbTableFile = new DbTableFile(new File("table_person.txt"));
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT));
        TableDesc tableDesc = new TableDesc(attributes);
        // tableDesc
        dataBase.addTable(dbTableFile, "t_person", tableDesc);

        this.dataBase = dataBase;
    }
    //
    // @Test
    // public void writePageToDisk() {
    //     DataBase dataBase = new DataBase();
    //     // 创建数据库文件
    //     // 路径修改
    //     DbTableFile dbTableFile = new DbTableFile(new File("db_file.txt"));
    //     List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT));
    //     TableDesc tableDesc = new TableDesc(attributes);
    //     // tableDesc
    //     dataBase.addTable(dbTableFile, "t_person", tableDesc);
    //     Page page = new Page(0, new byte[]{});
    //     // 数据写入file
    //     dbTableFile.writePageToDisk(page);
    // }

    @Test
    public void insertRow() throws IOException {
        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Tuple tuple = new Tuple(tablePerson.getTupleDesc());
        tablePerson.insertRow(tuple);
    }


    @Test
    public void insertRowTest() throws IOException {
        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Tuple tuple = new Tuple(tablePerson.getTupleDesc());
        tuple.setField(0, new IntField(17));
        tablePerson.insertRow(tuple);
    }
}
