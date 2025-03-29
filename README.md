# SQL Lineage Analyzer

A web-based tool for analyzing SQL lineage in BigQuery queries. This tool helps you understand table dependencies and column usage in your SQL queries.

## Features

- SQL Lineage Analysis
  - Table dependency detection
  - Column usage tracking
  - Support for complex SQL queries including:
    - WITH clauses (CTEs)
    - Subqueries
    - JOINs
    - UNION/UNION ALL
    - Table aliases
    - Field aliases

- User Interface
  - Split-screen layout
  - Real-time analysis
  - Responsive design
  - Syntax highlighting
  - Clear result presentation

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- Spring Boot 2.7.0

## Getting Started

1. Clone the repository:
```bash
git clone https://github.com/yourusername/sql-lineage-analyzer.git
cd sql-lineage-analyzer
```

2. Build the project:
```bash
mvn clean package
```

3. Run the application:
```bash
java -jar target/data-agent-1.0-SNAPSHOT.jar
```

4. Open your browser and navigate to:
```
http://localhost:8080
```

## Usage

1. Enter your SQL query in the left panel
2. Click "Analyze SQL" button
3. View the analysis results in the right panel

The analysis will show:
- Source tables used in the query
- Columns referenced from each table
- Table dependencies and relationships

## Example

Input SQL:
```sql
SELECT u.name, o.order_id, p.product_name 
FROM `project.dataset.users` u 
JOIN `project.dataset.orders` o ON u.id = o.user_id 
JOIN `project.dataset.products` p ON o.product_id = p.id 
WHERE u.age > 18
```

Analysis Result:
```
Table Dependencies:
==================

Source Table: project.dataset.users
Used Columns:
  - name
  - id
  - age

Source Table: project.dataset.orders
Used Columns:
  - order_id
  - user_id
  - product_id

Source Table: project.dataset.products
Used Columns:
  - product_name
  - id
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 