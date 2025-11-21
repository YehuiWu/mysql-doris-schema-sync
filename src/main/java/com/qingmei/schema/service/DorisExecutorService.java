package com.qingmei.schema.service;

import com.qingmei.schema.config.AppConfig;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

@Service
public class DorisExecutorService {
  private final AppConfig appConfig;

  public DorisExecutorService(AppConfig appConfig) {
    this.appConfig = appConfig;
  }

  public ExecResult execute(String sql) throws Exception {
    AppConfig.DorisConnInfo d = appConfig.getDoris();
    String url = "jdbc:mysql://" + d.getHost() + ":" + d.getPort() + "/" + d.getDatabase() + "?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai";
    try (Connection conn = DriverManager.getConnection(url, d.getUsername(), d.getPassword());
         Statement st = conn.createStatement()) {
      boolean ok = st.execute(sql);
      return new ExecResult(true, ok ? "执行成功" : "执行完成");
    } catch (Exception e) {
      return new ExecResult(false, e.getMessage());
    }
  }

  public static class ExecResult {
    public boolean success;
    public String message;
    public ExecResult(boolean success, String message) {
      this.success = success;
      this.message = message;
    }
  }
}