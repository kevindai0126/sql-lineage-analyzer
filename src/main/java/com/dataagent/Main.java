package com.dataagent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            // 创建SQL分析器
            SqlLineageAnalyzer analyzer = new SqlLineageAnalyzer();
            
            // 示例BigQuery SQL查询
            String sql = "SELECT " +
                        "  u.name, " +
                        "  u.age, " +
                        "  o.order_id, " +
                        "  o.amount, " +
                        "  p.product_name, " +
                        "  p.price, " +
                        "  COUNT(*) as total_orders " +
                        "FROM `project.dataset.users` u " +
                        "JOIN `project.dataset.orders` o ON u.id = o.user_id " +
                        "JOIN `project.dataset.products` p ON o.product_id = p.id " +
                        "WHERE u.age > 18 " +
                        "  AND o.status = 'active' " +
                        "GROUP BY u.name, u.age, o.order_id, o.amount, p.product_name, p.price";
            
            // 分析SQL
            analyzer.analyzeLineage(sql);
            
        } catch (Exception e) {
            log.error("Error in main: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
} 