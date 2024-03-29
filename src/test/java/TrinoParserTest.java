import dev.tungtv.QueryParser;
import dev.tungtv.TableStatic;
import dev.tungtv.TableStatic.CMD;
import dev.tungtv.TrinoParser;
import io.trino.sql.parser.ParsingException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class TrinoParserTest {
    private final QueryParser trinoParser = new TrinoParser();

    @Test
    public void select_simple() throws IOException {
        String stmt = Common.getStmtFromFile("trino_select.txt");
        Set<TableStatic> actual = trinoParser.parser(stmt);
//        trino_select_1
        Assert.assertEquals(1, actual.size());
        Assert.assertTrue(actual.contains(new TableStatic(
                CMD.SELECT_TABLE,
                "dbtest",
                "tblhihi")));
    }

    @Test
    public void select_with() throws IOException {
        String stmt = Common.getStmtFromFile("trino_select_1.txt");
        Set<TableStatic> actual = trinoParser.parser(stmt);
//        trino_select_1
        Assert.assertEquals(3, actual.size());
        Assert.assertTrue(actual.contains(new TableStatic(
                CMD.SELECT_TABLE,
                "transformation_prod",
                "source_ssk_ghtk_package_logs_action")));
    }

    @Test
    public void select_with_join() throws IOException {
        TableStatic[] expected = new TableStatic[]{
                new TableStatic(CMD.SELECT_TABLE, "transformation_prod", "source_ssk_ghtk_package_logs"),
                new TableStatic(CMD.SELECT_TABLE, "", "raw_pl"),
                new TableStatic(CMD.SELECT_TABLE, "", "step_1"),
                new TableStatic(CMD.SELECT_TABLE, "transformation_prod", "stg_ghtk__emp_profiles"),
                new TableStatic(CMD.SELECT_TABLE, "transformation_prod", "stg_ghtk__emp_positions")
        };
        String stmt = Common.getStmtFromFile("trino_select_2.txt");
        Set<TableStatic> actual = trinoParser.parser(stmt);
        //        trino_select_2
        Assert.assertEquals(5, actual.size());
        Assert.assertArrayEquals(expected, actual.toArray());

    }

    @Test
    public void create_as_select() throws IOException {
        String stmt = Common.getStmtFromFile("trino_create_as_select.txt");
        Set<TableStatic> actual = trinoParser.parser(stmt);
//        trino_select_1
        Assert.assertEquals(11, actual.size());
        Assert.assertTrue(actual.contains(new TableStatic(
                CMD.CREATE_TABLE,
                "das_report",
                "trangntt_bh")));
    }

    @Test
    public void insert() throws IOException {
        TableStatic[] expected = new TableStatic[]{
                new TableStatic(CMD.INSERT_TABLE, "access_logs", "k8s_to_security_sensitive_delta"),
                new TableStatic(CMD.SELECT_TABLE, "access_logs", "k8s_prod_cdc"),
        };
        String stmt = Common.getStmtFromFile("trino_insert.txt");
        Set<TableStatic> actual = trinoParser.parser(stmt);

        Assert.assertEquals(2, actual.size());
        Assert.assertArrayEquals(expected, actual.toArray());
    }

    @Test
//            (expected = ParsingException.class)
    public void test_parsing_exception() throws IOException {
        TableStatic expected = new TableStatic(TableStatic.CMD.ERROR, null, null);
        String stmt = Common.getStmtFromFile("trino_error_syntax.txt");
        Set<TableStatic> actual = trinoParser.parser(stmt);
        Assert.assertTrue(actual.contains(expected));
    }
}
