import dev.tungtv.ImpalaParser;
import dev.tungtv.TableStatic;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class ImpalaParserTest {
    private final ImpalaParser impalaParser = new ImpalaParser();

    @Test
    public void createTableStmt() throws IOException {
        String stmt = Common.getStmtFromFile("impala_create_as_select.txt");
        String stmt1 = Common.getStmtFromFile("impala_create_as_select_1.txt");

        Assert.assertEquals(6, impalaParser.parser(stmt).size());
        Assert.assertEquals(4, impalaParser.parser(stmt1).size());
    }

    @Test
    public void queryStmt() throws IOException {
        String stmt = Common.getStmtFromFile("impala_select_1.txt");
        Set<TableStatic> actual = impalaParser.parser(stmt);
        Assert.assertEquals(13, actual.size());
    }

    @Test
    public void insertStmt() throws IOException {
        String stmt = Common.getStmtFromFile("impala_insert.txt");
        Set<TableStatic> actual = impalaParser.parser(stmt);
        Assert.assertEquals(3, actual.size());
    }

}
