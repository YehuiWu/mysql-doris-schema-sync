package com.qingmei.schema.service;

import com.qingmei.schema.service.MySqlMetadataService.ColumnInfo;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class SqlGeneratorService {
  public String generateSql(Input input, List<ColumnInfo> mysqlColumns) {
    String db = input.mysqlDb;
    String table = input.mysqlTable;
    String suffix = tableSuffix(input);
    String dorisTable = suffix == null ? ("ods_" + db + "_" + table) : ("ods_" + db + "_" + table + "_" + suffix);

    List<String> fields = input.fields == null || input.fields.isEmpty()
        ? mysqlColumns.stream().map(c -> c.name).collect(Collectors.toList())
        : input.fields;
    Set<String> fieldSet = new LinkedHashSet<>(fields);
    List<ColumnInfo> colsToUse = mysqlColumns.stream().filter(c -> fieldSet.contains(c.name)).collect(Collectors.toList());

    List<String> pk = new ArrayList<>(input.uniqueKeys);
    pk = pk.stream().filter(fieldSet::contains).collect(Collectors.toList());
    if (input.partitioned) {
      String pcol = partitionCol(input.partitionType);
      if (!pk.contains(pcol)) pk.add(0, pcol);
      else {
        pk.remove(pcol);
        pk.add(0, pcol);
      }
    }

    List<String> orderCols = new ArrayList<>();
    if (input.partitioned) orderCols.add(partitionCol(input.partitionType));
    orderCols.addAll(pk.stream().filter(c -> !orderCols.contains(c)).collect(Collectors.toList()));

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(dorisTable).append(" (\n");
    if (input.partitioned) {
      sb.append("  ").append(partitionCol(input.partitionType)).append(" date");
      String pcmt = input.partitionType.equals("DAY") ? "日分区" : input.partitionType.equals("MONTH") ? "月分区" : "年分区";
      sb.append(" COMMENT '").append(pcmt).append("',\n");
    }
    for (int i = 0; i < colsToUse.size(); i++) {
      ColumnInfo c = colsToUse.get(i);
      String dtype = TypeMapper.toDorisType(c.dataType, c.charLen, c.numPrecision, c.numScale);
      sb.append("  `").append(c.name).append("` ").append(dtype);
      if (c.comment != null && !c.comment.isEmpty()) sb.append(" COMMENT '").append(escapeQuote(c.comment)).append("'");
      if (i < colsToUse.size() - 1 || !input.indexes.isEmpty()) sb.append(",\n"); else sb.append("\n");
    }
    List<String> idxCols = input.indexes == null ? Collections.emptyList() : input.indexes.stream().filter(fieldSet::contains).collect(Collectors.toList());
    for (int i = 0; i < idxCols.size(); i++) {
      String col = idxCols.get(i);
      sb.append("  INDEX index_").append(col).append(" (`").append(col).append("`) USING INVERTED");
      if (i < idxCols.size() - 1) sb.append(",\n"); else sb.append("\n");
    }
    sb.append(") ENGINE = OLAP\n");
    sb.append("UNIQUE KEY(");
    for (int i = 0; i < orderCols.size(); i++) {
      sb.append("`").append(orderCols.get(i)).append("`");
      if (i < orderCols.size() - 1) sb.append(",");
    }
    sb.append(")\n");
    if (input.tableComment != null && !input.tableComment.isEmpty()) sb.append("COMMENT '").append(escapeQuote(input.tableComment)).append("'\n");
    if (input.partitioned) {
      String pcol = partitionCol(input.partitionType);
      sb.append("PARTITION BY RANGE(`").append(pcol).append("`)()\n");
    }
    String distCol = input.distributionKey == null ? orderCols.get(0) : input.distributionKey;
    if (!fieldSet.contains(distCol)) distCol = orderCols.get(0);
    sb.append("DISTRIBUTED BY HASH(`").append(distCol).append("`) BUCKETS AUTO\n");
    sb.append("PROPERTIES (\n");
    if (input.partitioned) {
      sb.append("  \"dynamic_partition.enable\" = \"true\",\n");
      sb.append("  \"dynamic_partition.time_unit\" = \"").append(input.partitionType).append("\",\n");
      sb.append("  \"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n");
      sb.append("  \"dynamic_partition.start\" = \"-2\",\n");
      sb.append("  \"dynamic_partition.end\" = \"1\",\n");
      sb.append("  \"dynamic_partition.create_history_partition\" = \"false\",\n");
      sb.append("  \"dynamic_partition.prefix\" = \"p\",\n");
    }
    if (input.sequenceCol != null && !input.sequenceCol.isEmpty() && (input.sequenceCol.equals(partitionCol(input.partitionType)) || fieldSet.contains(input.sequenceCol))) {
      sb.append("  \"function_column.sequence_col\" = \"").append(input.sequenceCol).append("\",\n");
    }
    sb.append("  \"enable_unique_key_merge_on_write\" = \"true\"\n");
    sb.append(");");
    return sb.toString();
  }

  private String escapeQuote(String s) {
    return s.replace("'", "''");
  }

  private String partitionCol(String type) {
    switch (type) {
      case "DAY": return "dt";
      case "MONTH": return "mt";
      case "YEAR": return "yt";
      default: return "dt";
    }
  }

  private String tableSuffix(Input in) {
    if (!in.partitioned) return null;
    switch (in.partitionType) {
      case "DAY": return in.syncType.equals("df") ? "df" : "di";
      case "MONTH": return in.syncType.equals("mf") ? "mf" : "mi";
      case "YEAR": return null;
      default: return null;
    }
  }

  public static class Input {
    public String mysqlDs;
    public String mysqlDb;
    public String mysqlTable;
    public boolean partitioned;
    public String partitionType;
    public String syncType;
    public List<String> uniqueKeys = new ArrayList<>();
    public String distributionKey;
    public String sequenceCol;
    public List<String> indexes = new ArrayList<>();
    public String tableComment;
    public List<String> fields = new ArrayList<>();
  }
}