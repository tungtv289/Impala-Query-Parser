package dev.tungtv;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.impala.analysis.*;
import org.apache.impala.catalog.View;
import org.apache.log4j.Logger;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ImpalaParser extends QueryParser {
    final static Logger logger = Logger.getLogger(ImpalaParser.class);

    public Set<TableStatic> implParser(String stmt) throws Exception {
        Set<TableStatic> rs = new LinkedHashSet<>();
        //            List<String> rs = new LinkedList<>();
        SqlScanner input = new SqlScanner(new StringReader(stmt));
        SqlParser parser = new SqlParser(input);
        ParseNode node = null;
        node = (ParseNode) parser.parse().value;
        if (node instanceof QueryStmt) {
            rs.addAll(extractTableNamesFromQueryStmt((QueryStmt) node));
        } else if (node instanceof CreateTableAsSelectStmt) {
            rs.add(extractTableNamesFromCreateTableStmt(((CreateTableAsSelectStmt) node).getCreateStmt()));
            rs.addAll(extractTableNamesFromQueryStmt(((CreateTableAsSelectStmt) node).getQueryStmt()));
        } else if (node instanceof InsertStmt) {
            rs.add(extractTableNamesFromCreateTableStmt((InsertStmt) node));
            rs.addAll(extractTableNamesFromQueryStmt(((InsertStmt) node).getQueryStmt()));
        } else if(node instanceof ShowStatsStmt) {
            rs.add(extractTableNamesFromShowStatsStmt((ShowStatsStmt) node));
        }
        return rs;
    }

    public TableStatic extractTableNamesFromShowStatsStmt(ShowStatsStmt node) throws IllegalAccessException {
        Object fieldVal = FieldUtils.readField(node, "tableName_", true);
        String db = "";
        String tbl = "";
        if (fieldVal instanceof TableName) {
            db = ((TableName) fieldVal).getDb();
            tbl = ((TableName) fieldVal).getTbl();
        }
        return new TableStatic(TableStatic.CMD.SELECT_TABLE, db, tbl);
    }

    public TableStatic extractTableNamesFromCreateTableStmt(CreateTableStmt node) {
        return new TableStatic(TableStatic.CMD.CREATE_TABLE, node.getTblName().getDb(), node.getTblName().getTbl());
    }

    public TableStatic extractTableNamesFromCreateTableStmt(InsertStmt node) {
        return new TableStatic(TableStatic.CMD.INSERT_TABLE, node.getTargetTableName().getDb(), node.getTargetTableName().getTbl());
    }

    public List<TableStatic> extractTableNamesFromQueryStmt(QueryStmt node) {
        List<TableStatic> rs = new ArrayList<>();
        if (node instanceof SelectStmt) {
            rs.addAll(extractTableNamesFromSelectStmt((SelectStmt) node));
        } else if (node instanceof UnionStmt) {
            rs.addAll(extractTableNamesFromUnionStmt((UnionStmt) node));
        }
        return rs;
    }

    private List<TableStatic> extractTableNamesFromUnionStmt(UnionStmt node) {
        List<TableStatic> rs = new ArrayList<>();
        for (UnionStmt.UnionOperand stmt : node.getOperands()) {
            rs.addAll(extractTableNamesFromSelectStmt((SelectStmt) stmt.getQueryStmt()));
        }
        return rs;
    }

    private List<TableStatic> extractTableNamesFromSelectStmt(SelectStmt node) {
        List<TableStatic> rs = new ArrayList<>();
        for (TableRef tblRef : node.getTableRefs()) {
            if (tblRef instanceof InlineViewRef) {
                InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
                QueryStmt viewStmt = inlineViewRef.getViewStmt();
                if (viewStmt instanceof SelectStmt) {
                    rs.addAll(extractTableNamesFromSelectStmt((SelectStmt) viewStmt));
                } else if (viewStmt instanceof UnionStmt) {
                    rs.addAll(extractTableNamesFromUnionStmt((UnionStmt) viewStmt));
                }
            } else {
                String dbName = tblRef.getPath().size() > 1 ? tblRef.getPath().get(0) : "";
                String tblName = tblRef.getPath().size() > 1 ? tblRef.getPath().get(1) : tblRef.getPath().get(0);
                rs.add(new TableStatic(TableStatic.CMD.SELECT_TABLE, dbName, tblName));
            }
        }
        if (node.hasWithClause()) {
            for (View view : node.getWithClause().getViews()) {
                QueryStmt queryStmt = view.getQueryStmt();
                if (queryStmt instanceof SelectStmt)
                    rs.addAll(extractTableNamesFromSelectStmt((SelectStmt) queryStmt));
            }
        }
        return rs;
    }

}
