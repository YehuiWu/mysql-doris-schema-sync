package com.qingmei.schema.service;

import com.qingmei.schema.config.AppConfig;
import com.qingmei.schema.service.MySqlMetadataService.ColumnInfo;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class SqlGeneratorService {
  private final AppConfig appConfig;

  public SqlGeneratorService(AppConfig appConfig) {
    this.appConfig = appConfig;
  }
  public String generateSql(Input input, List<ColumnInfo> mysqlColumns) {
    String db = input.mysqlDb;
    String table = input.mysqlTable;
    String suffix = tableSuffix(input);
    String baseName = suffix == null ? ("ods_" + db + "_" + table) : ("ods_" + db + "_" + table + "_" + suffix);
    String dorisDb = appConfig.getDoris() != null && appConfig.getDoris().getDatabase() != null ? appConfig.getDoris().getDatabase() : "ods";
    String dorisTable = "`" + dorisDb + "`." + "`" + baseName + "`";

    List<String> fields = input.fields == null || input.fields.isEmpty()
        ? mysqlColumns.stream().map(c -> c.name).collect(Collectors.toList())
        : input.fields;
    Set<String> fieldSet = new LinkedHashSet<>(fields);
    List<ColumnInfo> colsToUse = mysqlColumns.stream().filter(c -> fieldSet.contains(c.name)).collect(Collectors.toList());

    List<String> pk = new ArrayList<>(input.uniqueKeys);
    pk = pk.stream().filter(fieldSet::contains).collect(Collectors.toList());
    String pcol = input.partitioned ? partitionCol(input.partitionType) : null;
    if (input.partitioned && pcol != null && pk.contains(pcol)) {
      pk = pk.stream().filter(k -> !k.equals(pcol)).collect(Collectors.toList());
    }

    List<String> orderCols = new ArrayList<>();
    if (input.partitioned && pcol != null) orderCols.add(pcol);
    for (ColumnInfo c : colsToUse) {
      if (pk.contains(c.name) && !orderCols.contains(c.name)) orderCols.add(c.name);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(dorisTable.replace(".", "."))
        .append(" (\n");
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
      String partCol = partitionCol(input.partitionType);
      sb.append("PARTITION BY RANGE(`").append(partCol).append("`)()\n");
    }
    String distCol = input.distributionKey == null ? orderCols.get(0) : input.distributionKey;
    if (!fieldSet.contains(distCol)) distCol = orderCols.get(0);
    if (input.bucketCount != null && input.bucketCount > 0) {
      sb.append("DISTRIBUTED BY HASH(`").append(distCol).append("`) BUCKETS ").append(input.bucketCount).append("\n");
    } else {
      sb.append("DISTRIBUTED BY HASH(`").append(distCol).append("`) BUCKETS AUTO\n");
    }
    sb.append("PROPERTIES (\n");
    if (input.partitioned) {
      String dStart = (input.dynamicStart != null && !input.dynamicStart.isEmpty()) ? input.dynamicStart : "-2";
      String dEnd = (input.dynamicEnd != null && !input.dynamicEnd.isEmpty()) ? input.dynamicEnd : "1";
      sb.append("  \"dynamic_partition.enable\" = \"true\",\n");
      sb.append("  \"dynamic_partition.time_unit\" = \"").append(input.partitionType).append("\",\n");
      String tz = (input.dpTimeZone != null && !input.dpTimeZone.isEmpty()) ? input.dpTimeZone : "Asia/Shanghai";
      sb.append("  \"dynamic_partition.time_zone\" = \"").append(tz).append("\",\n");
      sb.append("  \"dynamic_partition.start\" = \"").append(dStart).append("\",\n");
      sb.append("  \"dynamic_partition.end\" = \"").append(dEnd).append("\",\n");
      String createHistory = (input.dpCreateHistory != null && input.dpCreateHistory) ? "true" : "false";
      String dpPrefix = (input.dpPrefix != null && !input.dpPrefix.isEmpty()) ? input.dpPrefix : "p";
      sb.append("  \"dynamic_partition.create_history_partition\" = \"").append(createHistory).append("\",\n");
      sb.append("  \"dynamic_partition.prefix\" = \"").append(dpPrefix).append("\",\n");
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
      case "DAY": return "full".equals(in.syncType) ? "df" : "di";
      case "MONTH": return "full".equals(in.syncType) ? "mf" : "mi";
      case "YEAR": return "full".equals(in.syncType) ? "yf" : "yi";
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
    public String dynamicStart;
    public String dynamicEnd;
    public String dpPrefix;
    public Boolean dpCreateHistory;
    public String dpTimeZone;
    public Integer bucketCount;
  }
}