import dev.tungtv.ImpalaParser;
import dev.tungtv.QueryParser;
import dev.tungtv.TableStatic;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class ImpalaParserTest {
    private final QueryParser impalaParser = new ImpalaParser();

    @Test
    public void create_as_select() throws IOException {
        String stmt = Common.getStmtFromFile("impala_create_as_select.txt");
        String stmt1 = Common.getStmtFromFile("impala_create_as_select_1.txt");

        Set<TableStatic> rsStmt = impalaParser.parser(stmt);
        Set<TableStatic> rsStmt1 = impalaParser.parser(stmt1);

        Assert.assertEquals(6, rsStmt.size());
        Assert.assertEquals(4, rsStmt1.size());
        Assert.assertTrue(rsStmt.contains(new TableStatic(TableStatic.CMD.CREATE_TABLE, "das_report", "ingest_kpi_xlhh_lego_bag_pkgs_imex")));
        Assert.assertTrue(rsStmt1.contains(new TableStatic(TableStatic.CMD.SELECT_TABLE, "ghtk_datamart", "dimcod")));
    }

    @Test
    public void select_with_join() throws IOException {
        String impalaSelect1 = Common.getStmtFromFile("impala_select_1.txt");
        Set<TableStatic> actual1 = impalaParser.parser(impalaSelect1);
        String impalaSelect2 = Common.getStmtFromFile("impala_select_2.txt");
        Set<TableStatic> actual2 = impalaParser.parser(impalaSelect2);

        Assert.assertEquals(13, actual1.size());
        Assert.assertEquals(7, actual2.size());

        Assert.assertTrue(actual1.contains(new TableStatic(
                TableStatic.CMD.SELECT_TABLE, "db_test", "stations")));
        Assert.assertTrue(actual2.contains(new TableStatic(
                TableStatic.CMD.SELECT_TABLE, "ghtk", "provinces_with_region_name")));
    }

    @Test
    public void select_from_union() throws IOException {
        String impalaSelect1 = Common.getStmtFromFile("impala_select_from_union.txt");
        Set<TableStatic> actual1 = impalaParser.parser(impalaSelect1);

        Assert.assertEquals(3, actual1.size());

        Assert.assertTrue(actual1.contains(new TableStatic(
                TableStatic.CMD.SELECT_TABLE, "ghtk", "package_archives")));
    }

    @Test
    public void insert() throws IOException {
        String stmt = Common.getStmtFromFile("impala_insert.txt");
        Set<TableStatic> actual = impalaParser.parser(stmt);
        Assert.assertEquals(3, actual.size());
    }

}
