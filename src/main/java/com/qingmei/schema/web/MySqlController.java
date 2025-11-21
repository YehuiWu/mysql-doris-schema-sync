package com.qingmei.schema.web;

import com.qingmei.schema.service.MySqlMetadataService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/api/mysql")
public class MySqlController {
  private final MySqlMetadataService meta;

  public MySqlController(MySqlMetadataService meta) {
    this.meta = meta;
  }

  @GetMapping("/datasources")
  public ResponseEntity<Set<String>> datasources() {
    return ResponseEntity.ok(meta.listDatasources());
  }

  @GetMapping("/databases")
  public ResponseEntity<List<String>> databases(@RequestParam String ds) throws Exception {
    return ResponseEntity.ok(meta.listDatabases(ds));
  }

  @GetMapping("/tables")
  public ResponseEntity<List<String>> tables(@RequestParam String ds, @RequestParam String db) throws Exception {
    return ResponseEntity.ok(meta.listTables(ds, db));
  }

  @GetMapping("/table-columns")
  public ResponseEntity<List<MySqlMetadataService.ColumnInfo>> columns(@RequestParam String ds, @RequestParam String db, @RequestParam String table) throws Exception {
    return ResponseEntity.ok(meta.listColumns(ds, db, table));
  }

  @GetMapping("/table-comment")
  public ResponseEntity<String> tableComment(@RequestParam String ds, @RequestParam String db, @RequestParam String table) throws Exception {
    return ResponseEntity.ok(meta.getTableComment(ds, db, table));
  }
}