package com.qingmei.schema.web;

import com.qingmei.schema.service.MySqlMetadataService;
import com.qingmei.schema.service.SqlGeneratorService;
import com.qingmei.schema.service.SqlGeneratorService.Input;
import com.qingmei.schema.service.DorisExecutorService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class SqlController {
  private final SqlGeneratorService generator;
  private final MySqlMetadataService meta;
  private final DorisExecutorService doris;

  public SqlController(SqlGeneratorService generator, MySqlMetadataService meta, DorisExecutorService doris) {
    this.generator = generator;
    this.meta = meta;
    this.doris = doris;
  }

  @PostMapping("/sql/generate")
  public ResponseEntity<String> generate(@RequestBody Input input) throws Exception {
    List<MySqlMetadataService.ColumnInfo> cols = meta.listColumns(input.mysqlDs, input.mysqlDb, input.mysqlTable);
    if (input.tableComment == null || input.tableComment.isEmpty()) {
      String tc = meta.getTableComment(input.mysqlDs, input.mysqlDb, input.mysqlTable);
      input.tableComment = tc == null ? "" : tc;
    }
    String sql = generator.generateSql(input, cols);
    return ResponseEntity.ok(sql);
  }

  @PostMapping("/doris/sql")
  public ResponseEntity<DorisExecutorService.ExecResult> execute(@RequestBody String sql) throws Exception {
    return ResponseEntity.ok(doris.execute(sql));
  }
}