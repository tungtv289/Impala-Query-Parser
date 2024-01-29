//package dev.tungtv;
//
//import io.trino.sql.parser.ParsingOptions;
//import io.trino.sql.parser.SqlParser;
//import io.trino.sql.tree.*;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//public class TrinoParser {
//    public static void main(String[] args) throws IOException {
//        BufferedReader reader = new BufferedReader(
//                new FileReader("/Users/trantung/Project/ThreadPool/sql2.txt"));
//        StringBuilder stringBuilder = new StringBuilder();
//        String line = null;
//        String ls = System.getProperty("line.separator");
//        while ((line = reader.readLine()) != null) {
//            stringBuilder.append(line);
//            stringBuilder.append(ls);
//        }
//// delete the last new line separator
//        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
//        reader.close();
//
//        String sql = stringBuilder.toString();
////        String sql = "";
//        SqlParser SQL_PARSER = new SqlParser();
//        Statement a = SQL_PARSER.createStatement(sql, new ParsingOptions());
//        List<String> tbls = new ArrayList<>();
//        if (a instanceof Query) {
//            tbls = getTableName((Query) a);
//        }
//        System.out.println(tbls);
//    }
//
//    public static List<String> join(Join join) {
//        List<String> rs = new ArrayList<>();
//        if(join.getType().equals(Join.Type.LEFT)) {
//            List<Node> children = (List<Node>) join.getRight().getChildren();
//            for (Node node : children) {
//                if (node instanceof AliasedRelation) {
//                    String tbl = ((Table)(( AliasedRelation) node).getRelation()).getName().toString();
//                    rs.add(tbl);
//                }
//            }
//        }
//        return rs;
//    }
//
//    public static List<String> getTableName(Query a) {
//        List<String> rs = new ArrayList<>();
//
//        Relation queryBody = ((QuerySpecification) a.getQueryBody()).getFrom().get();
//        if (queryBody instanceof Table) {
//            String tbl = ((Table) queryBody).getName().toString();
//            rs.add(tbl);
//        } else if (queryBody instanceof TableSubquery) {
//            List<Node> children = ((TableSubquery) queryBody).getChildren();
//            for (Node node : children) {
//                if (node instanceof Query) {
//                    rs.addAll(getTableName((Query) node));
//                }
//            }
//        } else if (queryBody instanceof Join) {
//            Join join = (Join) queryBody;
//            rs.addAll(join(join));
//        }
//        if (a.getWith().isPresent()) {
//            With with = a.getWith().get();
//            for (WithQuery wQuery : with.getQueries()) {
//                rs.addAll(getTableName(wQuery.getQuery()));
//            }
//        }
//        return rs;
//    }
//}
