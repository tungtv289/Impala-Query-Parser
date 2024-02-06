package dev.tungtv;

import java.util.Objects;


public class TableStatic {
    public enum CMD {
        CREATE_TABLE,
        ALTER_TABLE,
        INSERT_TABLE,
        SELECT_TABLE,
        ERROR
    }

    public CMD cmd;
    public String dbName;
    public String tableName;

    public TableStatic(CMD cmd, String dbName, String tableName) {
        this.cmd = cmd;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableStatic that = (TableStatic) o;
        return cmd == that.cmd && Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cmd, dbName, tableName);
    }

    @Override
    public String toString() {
        return "cmd='" + cmd + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                '\n';
    }
}
