# SQL Lineage Analyzer

一个用于分析 BigQuery SQL 查询中表和字段使用情况的工具。

## 功能特点

- 支持 BigQuery SQL 语法
- 分析查询中使用的源表
- 分析每个表中使用的字段
- 支持表别名
- 支持字段的完整引用（如 `table.column`）

## 项目结构

```
DataAgent/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── dataagent/
│       │           ├── Main.java
│       │           └── SqlLineageAnalyzer.java
│       └── resources/
│           └── logback.xml
├── pom.xml
└── README.md
```

## 依赖

- Java 11 或更高版本
- Maven 3.6 或更高版本
- Lombok
- SLF4J + Logback

## 构建和运行

1. 克隆项目：
```bash
git clone https://github.com/yourusername/sql-lineage-analyzer.git
cd sql-lineage-analyzer
```

2. 构建项目：
```bash
mvn clean install
```

3. 运行程序：
```bash
mvn exec:java -Dexec.mainClass="com.dataagent.Main"
```

## 示例输出

```
SQL Analysis Results:
=====================

Source Table: project.dataset.users
Used Columns:
  - name
  - age
  - id

Source Table: project.dataset.orders
Used Columns:
  - order_id
  - amount
  - user_id
  - product_id
  - status

Source Table: project.dataset.products
Used Columns:
  - product_name
  - price
  - id
```

## 许可证

MIT License 