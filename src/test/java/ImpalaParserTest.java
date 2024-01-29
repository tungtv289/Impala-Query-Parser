import dev.tungtv.ImpalaParser;
import dev.tungtv.TableStatic;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class ImpalaParserTest {
    @Test
    public void createTableStmt() throws IOException {
        String stmt = Common.getStmtFromFile("impala_create_as_select.txt");
        Set<TableStatic> actual = ImpalaParser.parser(stmt);
        Assert.assertEquals(6, actual.size());
    }

    @Test
    public void queryStmt() throws IOException {
        String stmt = Common.getStmtFromFile("impala_select_1.txt");
        Set<TableStatic> actual = ImpalaParser.parser(stmt);
        Assert.assertEquals(13, actual.size());
    }

    @Test
    public void insertStmt() throws IOException {
        String stmt = Common.getStmtFromFile("impala_insert.txt");
        Set<TableStatic> actual = ImpalaParser.parser(stmt);
        Assert.assertEquals(3, actual.size());
    }

}
