package org.apache.impala.analysis;

import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class Wrapper {
    public static Set<String> extractTableNamesFromSelectStmt(SelectStmt node) {
        Set<String> rs = new LinkedHashSet<>();
        for (SelectListItem selectListItem : node.getSelectList().getItems()) {
            rs.addAll(extractColumnsFromExpr(selectListItem.getExpr()));
        }
        return rs;
    }

    public static Set<String> extractColumnsFromExpr(Expr expr) {
        Set<String> rs = new LinkedHashSet<>();
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            try {
                Object fieldVal = FieldUtils.readField(slotRef, "rawPath_", true);
                if (fieldVal instanceof List) {
                    String fieldValString = String.join(".", (List) fieldVal);
//                    System.out.println(fieldValString);
                    rs.add(fieldValString);
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        } else if (expr instanceof CaseExpr) {
            CaseExpr caseExpr = (CaseExpr) expr;
            for (Expr exprChirld : caseExpr.getChildren()) {
                rs.addAll(extractColumnsFromExpr(exprChirld));
            }
        }
        return rs;
    }
}
