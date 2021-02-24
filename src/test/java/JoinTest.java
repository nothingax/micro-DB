import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.operator.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * join
 *
 * @author zhangjw
 * @version 1.0
 */
public class JoinTest {
    public DataBase dataBase = DataBase.getInstance();

    @Before
    public void initDataBase() throws IOException {
        // person 表
        List<TableDesc.Attribute> attributes =
                Arrays.asList(new TableDesc.Attribute("person_id", FieldType.INT),
                        new TableDesc.Attribute("dept_id", FieldType.INT));
        TableDesc tableDesc = new TableDesc(attributes);
        TableFile tableFile = new TableFile(new File(UUID.randomUUID().toString()), tableDesc);
        dataBase.addTable(tableFile, "t_person", tableDesc);

        // 部门表，2个int字段
        TableDesc depTableDesc = new TableDesc(
                Arrays.asList(new TableDesc.Attribute("id", FieldType.INT)));
        TableFile depTableFile = new TableFile(new File(UUID.randomUUID().toString()), depTableDesc);
        dataBase.addTable(depTableFile, "t_dept", depTableDesc);


        // dept 表插入两行数据，dept_id 分别为1、2
        DbTable tableDept = this.dataBase.getDbTableByName("t_dept");
        TableDesc deptTableDesc = tableDept.getTableDesc();

        Row row1 = new Row(deptTableDesc);
        row1.setFields(Arrays.asList(new IntField(1)));
        tableDept.insertRow(row1);

        Row row2 = new Row(deptTableDesc);
        row2.setFields(Arrays.asList(new IntField(2)));
        tableDept.insertRow(row2);


        // person表插入三行数据
        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        TableDesc personTableDesc = tablePerson.getTableDesc();

        Row tRow1 = new Row(personTableDesc);
        tRow1.setFields(Arrays.asList(new IntField(100),new IntField(1)));
        tablePerson.insertRow(tRow1);

        Row tRow2 = new Row(personTableDesc);
        tRow2.setFields(Arrays.asList(new IntField(200),new IntField(2)));
        tablePerson.insertRow(tRow2);

        Row tRow3 = new Row(personTableDesc);
        tRow3.setFields(Arrays.asList(new IntField(201),new IntField(2)));
        tablePerson.insertRow(tRow3);

    }


    /**
     * 基于seqScan和Join实现的关联查询
     * sql:select * from t_person,t_dept where t_person.dept_id = t_dept.id
     */
    @Test
    public void testJoin() throws IOException {
        SeqScan personScan = new SeqScan(this.dataBase.getDbTableByName("t_person").getTableId());
        SeqScan deptScan = new SeqScan(this.dataBase.getDbTableByName("t_dept").getTableId());

        // select * from t_person,t_dept where t_person.dept_id = t_dept.id
        JoinPredicate joinPredicate = new JoinPredicate(1, 0, PredicateEnum.EQUALS);
        Join join = new Join(personScan, deptScan, joinPredicate);
        join.open();

        while (join.hasNext()) {
            Row next = join.next();
            System.out.println(next.toString());
        }

        join.close();

    }
}
