package com.qingmei.schema.service;

import com.qingmei.schema.service.MySqlMetadataService.ColumnInfo;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class SqlGeneratorServiceTest {
  @Test
  void generateDayPartitionUnique() {
    SqlGeneratorService s = new SqlGeneratorService();
    SqlGeneratorService.Input in = new SqlGeneratorService.Input();
    in.mysqlDs = "pxs";
    in.mysqlDb = "shop";
    in.mysqlTable = "goods";
    in.partitioned = true;
    in.partitionType = "DAY";
    in.syncType = "di";
    in.uniqueKeys = Arrays.asList("tb_id","data_type","shop_id","sku_code","goods_id");
    in.distributionKey = "tb_id";
    in.sequenceCol = "insert_time";
    in.indexes = Arrays.asList("tb_id","insert_time");

    List<ColumnInfo> cols = new ArrayList<>();
    cols.add(ci("tb_id","int",null, null, null, true));
    cols.add(ci("data_type","int",null, null, null, false));
    cols.add(ci("shop_id","varchar",100L, null, null, false));
    cols.add(ci("sku_code","varchar",100L, null, null, false));
    cols.add(ci("goods_id","varchar",100L, null, null, false));
    cols.add(ci("insert_time","datetime",null, null, null, false));

    String sql = s.generateSql(in, cols);
    assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS ods_shop_goods_di"));
    assertTrue(sql.contains("UNIQUE KEY(`dt`,`tb_id"));
    assertTrue(sql.contains("\"enable_unique_key_merge_on_write\" = \"true\""));
  }

  private ColumnInfo ci(String n, String dt, Long len, Integer p, Integer s, boolean pk) {
    ColumnInfo c = new ColumnInfo();
    c.name = n; c.dataType = dt; c.charLen = len; c.numPrecision = p; c.numScale = s; c.isPrimaryKey = pk; c.nullable = true; c.comment = ""; return c;
  }
}