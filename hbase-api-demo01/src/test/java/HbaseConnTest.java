import com.flamingo.bigdata.hbase.api.HbaseConn;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;

/**
 * @program: hbase-example
 * @Date: 2019/1/13 21:46
 * @Author: Qinhs
 * @Description:
 */
public class HbaseConnTest {

    @Test
    public void getConnTest(){
        Connection conn = HbaseConn.getHbaseConn();
        System.out.println(conn.isClosed());
        HbaseConn.closeConn();
        System.out.println(conn.isClosed());
    }

    @Test
    public void getTableTest(){
        try {
            Table table = HbaseConn.getTable("US_POPULATION");
            System.out.println(table.getName().getNameAsString());
            table.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
