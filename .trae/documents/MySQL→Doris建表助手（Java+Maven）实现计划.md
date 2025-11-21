**目标**

* 提供一个Java+Maven的Web页面：左上角选择MySQL数据源/库/表；左下角配置Doris建表；右侧实时生成并预览Doris建表SQL；支持一键在Doris执行。

**总体架构**

* 后端：Spring Boot（Maven构建），REST接口，JDBC直连MySQL与Doris（Doris以MySQL协议端口执行DDL）。

* 前端：Thymeleaf + 原生JS（或轻量前端）实现三联动下拉与配置面板、SQL预览与执行按钮。

* 配置：`application.yml`中维护多数据源参数（来源于你提供的JSON），支持环境变量覆盖。

**后端模块**

* `datasource`：动态构建与缓存多个MySQL数据源连接（HikariCP），按别名选择连接。

* `metadata`：MySQL元数据查询服务

  * 列出数据源：`GET /api/mysql/datasources`

  * 列出库：`GET /api/mysql/databases?ds=pxs`

  * 列出表：`GET /api/mysql/tables?ds=pxs&db=xxx`

  * 列出字段：`GET /api/mysql/table-columns?ds=pxs&db=xxx&table=yyy`

* `sql-generator`：根据选择的MySQL表元数据和Doris配置生成`CREATE TABLE`语句

  * 保证Unique模型、分区键位于Key列最前、字段顺序与类型映射规则。

  * 生成表名：

    * 非分区：`ods_<库>_<表>`

    * 分区+同步方式：`ods_<库>_<表>_di`（日增量），`_df`（日全量），`_mi`（月增量），`_mf`（月全量）。

* `doris-executor`：Doris执行服务

  * `POST /api/doris/sql` 执行DDL（使用JDBC连接 `jdbc:mysql://192.168.11.50:49010/ods`）。

* `validation`：校验Key列、分区规则、字段映射安全边界等。

**前端页面布局**

* 左上（MySQL数据源）

  * 下拉1：数据源别名（pxs、pos、xja、dcs、pfm、ags、stl、pbi、clm）。

  * 下拉2：库名（联动加载）。

  * 下拉3：表名（联动加载）。

  * 字段列表预览（含MySQL类型）。

* 左下（Doris建表配置）

  * 是否分区：`非分区`/`分区`。

  * 分区类型：`日(dt)`/`月(mt)`/`年(yt)`；分区字段自动为对应命名，类型为`date`。

  * 同步方式：`di/df/mi/mf`（根据分区类型过滤可选项）。

  * Unique Key选择：

    * 自动取MySQL主键；若无主键，提供多选字段以定义主键或联合主键。

    * 分区键必须在Key列内，且置于最前（例如分区日：`dt`在最前）。

  * 分布列：下拉选择一个字段（默认首个Key列）。

  * Sequence列：下拉选择一个MySQL字段（默认优先`insert_time`、`update_time`、`ts`等）。

  * 索引：可多选字段生成`USING INVERTED`索引。

  * 通用属性：

    * `"enable_unique_key_merge_on_write" = "true"` 固定开启。

    * 动态分区属性（当分区开启）：

      * `dynamic_partition.enable = true`

      * `time_unit = DAY|MONTH|YEAR`

      * `time_zone = Asia/Shanghai`

      * `start = -2`，`end = 1`

      * `create_history_partition = false`

      * `prefix = p`

      * `function_column.sequence_col = <选定列>`

* 右侧（SQL预览与执行）

  * 实时展示生成的`CREATE TABLE`语句。

  * `复制SQL`、`执行到Doris`、执行结果与错误反馈。

**SQL生成规则**

* 表模型：全部使用`ENGINE = OLAP` + `UNIQUE KEY (...)`。

* Key列顺序：

  * 若分区：分区键在最前，其后为主键或联合主键其余列。

  * 若非分区：仅主键或联合主键，按所选顺序。

* 分布策略：`DISTRIBUTED BY HASH(<分布列>) BUCKETS AUTO`。

* 分区声明：

  * 日分区：`PARTITION BY RANGE(dt)(...)`，`time_unit=DAY`。

  * 月分区：`PARTITION BY RANGE(mt)(...)`，`time_unit=MONTH`。

  * 年分区：`PARTITION BY RANGE(yt)(...)`，`time_unit=YEAR`。

* 索引：为所选字段生成 `INDEX index_<col> (`<col>`) USING INVERTED`。

* 字段类型映射（MySQL→Doris）：

  * `varchar(n)` → `varchar(n*3)`（上限适配Doris约束，超限则转`string`）。

  * `char(n)` → `char(n*3)`（同上限处理）。

  * `text/mediumtext/longtext` → `string`。

  * `bit`、`bool/boolean` → `int`。

  * `tinyint/smallint/int/bigint` → 同名整数类型。

  * `decimal(p,s)` → `decimal(p,s)`。

  * `float/double` → 同名类型。

  * `date` → `date`；`datetime/timestamp` → `datetime(6)`。

  * `binary/varbinary/blob` → `string`（十六进制或Base64场景用字符串存储）。

  * `enum/set` → `string`。

* 列注释：保留MySQL列注释（若有），并支持自定义表注释。

* 生成SQL不包含注释行，避免属性块内注释导致解析问题。

**对你提供的示例SQL的评估**

* 符合Unique模型与动态分区的要求；`dt`置于Key列最前，`function_column.sequence_col = "insert_time"`、`enable_unique_key_merge_on_write = true`合理。

* 我们的生成器将按你的命名规范改造（使用`ods_库_表_后缀`），并将索引生成与分布列选择改为可配置。

**安全与配置**

* 密码仅存服务器端配置，接口不回传任何凭据。

* 连接池限流与超时、SQL输入校验、防止DDL注入：对库/表/字段名进行白名单与引用转义。

* 所有外部地址与凭据可通过环境变量覆盖。

**测试与验证**

* 单元测试：类型映射、Key列排序、分区命名生成、表名后缀生成。

* 集成测试：模拟元数据响应、生成SQL一致性、Doris执行接口成功/失败用例。

* 手动验证：在页面选择不同数据源与分区策略，观察SQL预览与执行结果。

**交付内容**

* Maven项目：Spring Boot后端、Thymeleaf前端页面、配置文件。

* 文档：接口说明与简单使用步骤（启动、访问、示例）。

* 可运行演示：本地启动后在浏览器访问并连接到你提供的MySQL与Doris。

**实施步骤**

1. 初始化Spring Boot Maven工程与基础依赖（web、jdbc、HikariCP、mysql-connector-j、thymeleaf）。
2. 编写数据源与元数据服务，完成三联动下拉接口。
3. 实现SQL生成器与规则（类型映射、Key列、分区、索引、属性）。
4. 实现Doris执行接口（JDBC），返回执行状态与错误信息。
5. 构建前端页面（布局、交互、预览、执行）。
6. 编写测试与校验、完善异常处理与安全控制。

