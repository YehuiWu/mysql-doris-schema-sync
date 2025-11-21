package com.qingmei.schema.service;

import com.qingmei.schema.config.AppConfig;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

@Service
public class MySqlMetadataService {
  private final AppConfig appConfig;

  public MySqlMetadataService(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  public Set<String> listDatasources() {
    return appConfig.getMysql().keySet();
  }

  public List<String> listDatabases(String ds) throws Exception {
    try (Connection conn = openConnection(ds, "")) {
      List<String> dbs = new ArrayList<>();
      try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SHOW DATABASES")) {
        while (rs.next()) dbs.add(rs.getString(1));
      }
      return dbs;
    }
  }

  public List<String> listTables(String ds, String db) throws Exception {
    try (Connection conn = openConnection(ds, db)) {
      List<String> tables = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME")) {
        ps.setString(1, db);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) tables.add(rs.getString(1));
        }
      }
      return tables;
    }
  }

  public List<ColumnInfo> listColumns(String ds, String db, String table) throws Exception {
    try (Connection conn = openConnection(ds, db)) {
      List<ColumnInfo> cols = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_COMMENT, COLUMN_KEY " +
              "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION")) {
        ps.setString(1, db);
        ps.setString(2, table);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            ColumnInfo c = new ColumnInfo();
            c.name = rs.getString(1);
            c.dataType = rs.getString(2);
            c.charLen = rs.getObject(3) == null ? null : rs.getLong(3);
            c.numPrecision = rs.getObject(4) == null ? null : rs.getInt(4);
            c.numScale = rs.getObject(5) == null ? null : rs.getInt(5);
            c.nullable = "YES".equalsIgnoreCase(rs.getString(6));
            c.comment = rs.getString(7);
            c.isPrimaryKey = "PRI".equalsIgnoreCase(rs.getString(8));
            cols.add(c);
          }
        }
      }
      return cols;
    }
  }

  public String getTableComment(String ds, String db, String table) throws Exception {
    try (Connection conn = openConnection(ds, db)) {
      try (PreparedStatement ps = conn.prepareStatement("SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?")) {
        ps.setString(1, db);
        ps.setString(2, table);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            String c = rs.getString(1);
            return c == null ? "" : c;
          }
        }
      }
      return "";
    }
  }

  private Connection openConnection(String ds, String db) throws Exception {
    AppConfig.MySqlConnInfo info = appConfig.getMysql().get(ds);
    String url = "jdbc:mysql://" + info.getHost() + ":" + info.getPort() + "/" + (db == null ? "" : db) + "?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai";
    return DriverManager.getConnection(url, info.getUsername(), info.getPassword());
  }

  public static class ColumnInfo {
    public String name;
    public String dataType;
    public Long charLen;
    public Integer numPrecision;
    public Integer numScale;
    public boolean nullable;
    public String comment;
    public boolean isPrimaryKey;
  }
}