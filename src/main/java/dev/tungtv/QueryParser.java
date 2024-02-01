package dev.tungtv;

import org.apache.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.Set;

public abstract class QueryParser {
    final static Logger logger = Logger.getLogger(QueryParser.class);

    public Set<TableStatic> parser(String query) {
        Set<TableStatic> rs = new LinkedHashSet<>();
        try {
            rs.addAll(this.implParser(query));
        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
            logger.error(query);
            rs.add(new TableStatic(TableStatic.CMD.ERROR, null, null));
        }
        logger.debug(rs);
        return rs;
    }

    protected abstract Set<TableStatic> implParser(String query) throws Exception;
}
