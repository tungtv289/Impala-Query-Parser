package dev.tungtv;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.*;
import org.apache.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class TrinoParser {
    final static Logger logger = Logger.getLogger(TrinoParser.class);

    public Set<TableStatic> parser(String query) {
        Set<TableStatic> rs = new LinkedHashSet<>();
        SqlParser sqlParser = new SqlParser();
        Statement stmt = sqlParser.createStatement(query, new ParsingOptions());
        try {
            if (stmt instanceof Query) {
                rs.addAll(extractTableNamesFromNode(stmt));
            } else if (stmt instanceof CreateTableAsSelect) {
                rs.add(extractTableNamesFromCreateTableStmt((CreateTableAsSelect) stmt));
                rs.addAll(extractTableNamesFromNode(((CreateTableAsSelect) stmt).getQuery()));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        logger.debug(rs);
        return rs;
    }

    public TableStatic extractTableNamesFromCreateTableStmt(CreateTableAsSelect stmt) throws Exception {
        return qualifiedName2TableStatic(TableStatic.CMD.CREATE_TABLE, stmt.getName());
    }

    public Set<TableStatic> extractTableNamesFromNode(Node stmt) throws Exception {
        Set<TableStatic> rs = new LinkedHashSet<>();
        for (Node node : stmt.getChildren()) {
            if (node instanceof Table) {
                rs.add(trinoTable2TableStatic(TableStatic.CMD.SELECT_TABLE, (Table) node));
            } else {
                rs.addAll(extractTableNamesFromNode(node));
            }
        }
        return rs;
    }

//    public Set<TableStatic> extractTableNamesFromQueryStmt(Query stmt) throws Exception {
//        Set<TableStatic> rs = new LinkedHashSet<>();
//        Relation queryBody = ((QuerySpecification) stmt.getQueryBody()).getFrom().get();
//        if (queryBody instanceof Table) {
//            rs.add(trinoTable2TableStatic(TableStatic.CMD.SELECT_TABLE, (Table) queryBody));
//        } else if (queryBody instanceof TableSubquery) {
//            List<Node> children = ((TableSubquery) queryBody).getChildren();
//            for (Node node : children) {
//                if (node instanceof Query) {
//                    rs.addAll(extractTableNamesFromQueryStmt((Query) node));
//                }
//            }
//        } else if (queryBody instanceof Join) {
//            Join join = (Join) queryBody;
//            rs.addAll(extractTableNamesFromJoinStmt(join));
//        }
//        if (stmt.getWith().isPresent()) {
//            With with = stmt.getWith().get();
//            for (WithQuery wQuery : with.getQueries()) {
//                rs.addAll(extractTableNamesFromQueryStmt(wQuery.getQuery()));
//            }
//        }
//        return rs;
//    }

    public Set<TableStatic> extractTableNamesFromJoinStmt(Join join) throws Exception {
        Set<TableStatic> rs = new LinkedHashSet<>();
        if (join.getType().equals(Join.Type.LEFT)) {
            List<Node> children = (List<Node>) join.getRight().getChildren();
            for (Node node : children) {
                if (node instanceof AliasedRelation) {
                    Table tbl = (Table) ((AliasedRelation) node).getRelation();
                    rs.add(trinoTable2TableStatic(TableStatic.CMD.SELECT_TABLE, tbl));
                }
            }
        }
        return rs;
    }

    private TableStatic trinoTable2TableStatic(TableStatic.CMD cmd, Table table) throws Exception {
        QualifiedName qualifiedName = table.getName();
        return qualifiedName2TableStatic(cmd, qualifiedName);
    }

    private TableStatic qualifiedName2TableStatic(TableStatic.CMD cmd, QualifiedName qualifiedName) throws Exception {
        if (qualifiedName.getParts().size() == 1) {
            return new TableStatic(cmd, "", qualifiedName.getParts().get(0));
        } else if (qualifiedName.getParts().size() == 3) {
            return new TableStatic(cmd, qualifiedName.getParts().get(1), qualifiedName.getParts().get(2));
        } else if (qualifiedName.getParts().size() == 2) {
            return new TableStatic(cmd, qualifiedName.getParts().get(0), qualifiedName.getParts().get(1));
        } else throw new Exception("Invalid table qualifiedName");
    }
}
