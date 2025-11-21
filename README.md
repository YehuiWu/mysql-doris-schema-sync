# MySQL → Doris 建表助手（Java + Maven）

一个基于 Spring Boot 的轻量网页工具：从多个 MySQL 数据源选择库/表，在页面左下配置 Doris 建表规则，右侧实时生成并预览 SQL，支持一键执行到 Doris。

## 特性
- 多数据源：选择数据源 → 库 → 表，联动加载字段
- Doris 生成：Unique 模型、分区键置前（dt/mt/yt）、分布列选择、Sequence 列选择、倒排索引生成
- 命名规范：非分区 `ods_<库>_<表>`；分区后缀 `di/df/mi/mf`
- 类型映射：`varchar(n)`→`varchar(3n)`（超限转 `string`）、`bit/boolean`→`int`、`datetime/timestamp`→`datetime(6)` 等
- 表注释：默认使用 MySQL 表注释并支持在页面修改
- 一键执行：直接用 Doris FE 的 MySQL 协议端口执行 DDL

## 环境要求
- JDK 8
- Maven 3.6+

## 本地运行
1. 配置
   - 复制 `src/main/resources/application-example.yml` 为 `src/main/resources/application.yml`
   - 填入你的 MySQL 与 Doris 连接信息（生产环境建议通过环境变量覆盖）
2. 构建
   - `mvn -DskipTests package`
3. 启动
   - `java -jar target/mysql-doris-schema-sync-0.1.0.jar`
4. 访问
   - 打开浏览器访问 `http://localhost:8080/`

## 使用步骤
- 左上：选择数据源、库、表，字段默认全选；自动勾选主键为 Unique Key（无主键则不勾选）
- 左下：
  - 勾选是否分区，选择分区类型（DAY/MONTH/YEAR）
  - 选择同步后缀（di/df/mi/mf）
  - 选择分布列（默认第一个 Unique Key）
  - 选择 Sequence 列（默认优先 `insert_time`→`update_time`→`ts`）
  - 勾选倒排索引字段；可编辑“表注释”（初始为 MySQL 表注释）
- 右侧：点击“预览SQL”即可刷新生成的建表语句；可复制或执行到 Doris

## 主要接口
- 列出数据源：`GET /api/mysql/datasources`
- 列出库：`GET /api/mysql/databases?ds=...`
- 列出表：`GET /api/mysql/tables?ds=...&db=...`
- 列出字段：`GET /api/mysql/table-columns?ds=...&db=...&table=...`
- 表注释：`GET /api/mysql/table-comment?ds=...&db=...&table=...`
- 生成 SQL：`POST /api/sql/generate`
- 执行到 Doris：`POST /api/doris/sql`

## Doris 连接说明
- 使用 Doris FE 的 MySQL 协议端口（示例：`49010`），库名例如 `ods`
- 执行前请确保目标库存在

## 安全与配置
- `application.yml` 不会被提交到 Git（见 `.gitignore`）；提供 `application-example.yml` 作为模板
- 建议通过环境变量覆盖敏感信息（主机/端口/用户名/密码）
- 对库/表/字段名进行转义与校验，防止 DDL 注入

## 许可
- 本项目用于内部数据建表与同步辅助，可根据需要调整与扩展