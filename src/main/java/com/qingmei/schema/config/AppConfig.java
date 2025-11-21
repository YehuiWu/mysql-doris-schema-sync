package com.qingmei.schema.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "app")
public class AppConfig {
  private Map<String, MySqlConnInfo> mysql;
  private DorisConnInfo doris;

  public Map<String, MySqlConnInfo> getMysql() {
    return mysql;
  }

  public void setMysql(Map<String, MySqlConnInfo> mysql) {
    this.mysql = mysql;
  }

  public DorisConnInfo getDoris() {
    return doris;
  }

  public void setDoris(DorisConnInfo doris) {
    this.doris = doris;
  }

  public static class MySqlConnInfo {
    private String host;
    private int port;
    private String username;
    private String password;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
  }

  public static class DorisConnInfo {
    private String host;
    private int port;
    private String database;
    private String username;
    private String password;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
  }
}