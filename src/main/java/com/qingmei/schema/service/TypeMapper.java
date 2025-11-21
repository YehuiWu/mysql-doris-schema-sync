package com.qingmei.schema.service;

import java.util.Locale;

public class TypeMapper {
  public static String toDorisType(String mysqlType, Long charLen, Integer precision, Integer scale) {
    String t = mysqlType == null ? "" : mysqlType.toLowerCase(Locale.ROOT);
    switch (t) {
      case "varchar":
        long vlen = charLen == null ? 100L : charLen;
        long dlen = vlen * 3;
        if (dlen > 65533) return "string";
        return "varchar(" + dlen + ")";
      case "char":
        long clen = charLen == null ? 10L : charLen;
        long cdlen = clen * 3;
        if (cdlen > 255) return "string";
        return "char(" + cdlen + ")";
      case "text":
      case "mediumtext":
      case "longtext":
      case "tinytext":
        return "string";
      case "bit":
      case "boolean":
        return "int";
      case "tinyint":
        return "tinyint";
      case "smallint":
        return "smallint";
      case "int":
      case "integer":
        return "int";
      case "bigint":
        return "bigint";
      case "float":
        return "float";
      case "double":
        return "double";
      case "decimal":
        int p = precision == null ? 10 : precision;
        int s = scale == null ? 0 : scale;
        return "decimal(" + p + "," + s + ")";
      case "date":
        return "date";
      case "datetime":
      case "timestamp":
        return "datetime(6)";
      case "binary":
      case "varbinary":
      case "blob":
        return "string";
      case "enum":
      case "set":
        return "string";
      default:
        return "string";
    }
  }
}