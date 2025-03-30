package com.dataagent;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.anyString;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import com.dataagent.service.DataHubService;

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = {
    "datahub.environment=local"
})
class SqlLineageAnalyzerTest {

    @Mock
    private DataHubService dataHubService;

    @InjectMocks
    private SqlLineageAnalyzer sqlLineageAnalyzer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        // 设置默认的mock行为
        Map<String, String> defaultSchema = new HashMap<>();
        defaultSchema.put("id", "STRING");
        defaultSchema.put("name", "STRING");
        defaultSchema.put("email", "STRING");
        defaultSchema.put("created_at", "TIMESTAMP");
        defaultSchema.put("order_id", "STRING");
        defaultSchema.put("user_id", "STRING");
        defaultSchema.put("amount", "FLOAT");
        defaultSchema.put("status", "STRING");
        defaultSchema.put("product_id", "STRING");
        defaultSchema.put("quantity", "INTEGER");
        defaultSchema.put("unit_price", "DECIMAL");
        defaultSchema.put("category", "STRING");
        defaultSchema.put("in_stock", "BOOLEAN");
        
        when(dataHubService.getTableSchema(anyString(), anyString(), anyString()))
            .thenReturn(defaultSchema);
    }

    @Test
    void testAnalyzeSimpleSelect() {
        String sql = "SELECT id, name FROM test-project.test-dataset.users";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        System.out.println("Result:" + result.toString());
        assertNotNull(result);
        assertTrue(result.contains("Table Dependencies"));
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("Used Columns"));
        assertTrue(result.contains("id"));
        assertTrue(result.contains("name"));
    }

    @Test
    void testAnalyzeWithJoin() {
        String sql = "SELECT u.id, u.name, o.order_id " +
                    "FROM test-project.test-dataset.users u " +
                    "JOIN test-project.test-dataset.orders o ON u.id = o.user_id";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("id"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("order_id"));
    }

    @Test
    void testAnalyzeWithCTE() {
        String sql = "WITH user_orders AS (" +
                    "  SELECT user_id, COUNT(*) as order_count " +
                    "  FROM test-project.test-dataset.orders " +
                    "  GROUP BY user_id" +
                    ") " +
                    "SELECT u.name, uo.order_count " +
                    "FROM test-project.test-dataset.users u " +
                    "JOIN user_orders uo ON u.id = uo.user_id";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        System.out.println("Result:" + result.toString());
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("user_id"));
        
        assertFalse(result.contains("user_orders"));
        assertFalse(result.contains("order_count"));
    }

    @Test
    void testAnalyzeWithSubquery() {
        String sql = "SELECT u.name, o.order_id " +
                    "FROM test-project.test-dataset.users u " +
                    "JOIN test-project.test-dataset.orders o ON u.id = o.user_id " +
                    "WHERE o.amount > (SELECT AVG(amount) FROM test-project.test-dataset.orders)";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("order_id"));
        assertTrue(result.contains("amount"));
        
        assertFalse(result.contains("AVG(amount)"));
    }

    @Test
    void testAnalyzeWithUnion() {
        String sql = "SELECT id, name FROM test-project.test-dataset.users " +
                    "UNION ALL " +
                    "SELECT id, name FROM test-project.test-dataset.archived_users";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.archived_users"));
        assertTrue(result.contains("id"));
        assertTrue(result.contains("name"));
    }

    @Test
    void testAnalyzeWithSchemaInfo() {
        // 为特定表设置特定的schema信息
        Map<String, String> userSchema = new HashMap<>();
        userSchema.put("id", "STRING");
        userSchema.put("name", "STRING");
        userSchema.put("email", "STRING");
        when(dataHubService.getTableSchema("test-project", "test-dataset", "users"))
            .thenReturn(userSchema);

        String sql = "SELECT id, name FROM test-project.test-dataset.users";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("Schema Information"));
        assertTrue(result.contains("id (STRING)"));
        assertTrue(result.contains("name (STRING)"));
    }

    @Test
    void testAnalyzeWithComplexQuery() {
        String sql = "WITH user_stats AS (" +
                    "  SELECT user_id, COUNT(*) as order_count, SUM(amount) as total_amount " +
                    "  FROM test-project.test-dataset.orders " +
                    "  GROUP BY user_id" +
                    "), " +
                    "product_stats AS (" +
                    "  SELECT product_id, COUNT(*) as sales_count " +
                    "  FROM test-project.test-dataset.order_items " +
                    "  GROUP BY product_id" +
                    ") " +
                    "SELECT u.name, us.order_count, us.total_amount, " +
                    "       p.name as product_name, ps.sales_count " +
                    "FROM test-project.test-dataset.users u " +
                    "JOIN user_stats us ON u.id = us.user_id " +
                    "JOIN test-project.test-dataset.order_items oi ON u.id = oi.order_id " +
                    "JOIN test-project.test-dataset.products p ON oi.product_id = p.product_id " +
                    "JOIN product_stats ps ON p.product_id = ps.product_id";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        System.out.println("Result:" + result.toString());
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("test-project.test-dataset.order_items"));
        assertTrue(result.contains("test-project.test-dataset.products"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("user_id"));
        assertTrue(result.contains("product_id"));
        assertTrue(result.contains("amount"));
        
        assertFalse(result.contains("user_stats"));
        assertFalse(result.contains("product_stats"));
        assertFalse(result.contains("order_count"));
        assertFalse(result.contains("total_amount"));
        assertFalse(result.contains("sales_count"));
    }

    @Test
    void testAnalyzeWithComments() {
        String sql = "-- Get user information with their orders\n" +
                    "SELECT u.id, u.name, o.order_id\n" +
                    "FROM test-project.test-dataset.users u\n" +
                    "JOIN test-project.test-dataset.orders o ON u.id = o.user_id\n" +
                    "-- Filter for recent orders\n" +
                    "WHERE o.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("id"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("order_id"));
        assertTrue(result.contains("created_at"));
    }

    @Test
    void testAnalyzeWithMultipleCTEs() {
        String sql = "WITH first_cte AS (" +
                    "  SELECT user_id, COUNT(*) as order_count " +
                    "  FROM test-project.test-dataset.orders " +
                    "  GROUP BY user_id" +
                    "), " +
                    "second_cte AS (" +
                    "  SELECT product_id, SUM(quantity) as total_quantity " +
                    "  FROM test-project.test-dataset.order_items " +
                    "  GROUP BY product_id" +
                    ") " +
                    "SELECT u.name, fc.order_count, sc.total_quantity " +
                    "FROM test-project.test-dataset.users u " +
                    "JOIN first_cte fc ON u.id = fc.user_id " +
                    "JOIN test-project.test-dataset.order_items oi ON u.id = oi.order_id " +
                    "JOIN second_cte sc ON oi.product_id = sc.product_id";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        System.out.println("Result:" + result.toString());
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("test-project.test-dataset.order_items"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("user_id"));
        assertTrue(result.contains("product_id"));
        
        assertFalse(result.contains("first_cte"));
        assertFalse(result.contains("second_cte"));
        assertFalse(result.contains("order_count"));
        assertFalse(result.contains("total_quantity"));
    }

    @Test
    void testAnalyzeWithNestedSubqueries() {
        String sql = "SELECT u.name, o.order_id " +
                    "FROM test-project.test-dataset.users u " +
                    "JOIN test-project.test-dataset.orders o ON u.id = o.user_id " +
                    "WHERE o.amount > (" +
                    "  SELECT AVG(amount) " +
                    "  FROM test-project.test-dataset.orders " +
                    "  WHERE created_at > (" +
                    "    SELECT MAX(created_at) - INTERVAL 30 DAY " +
                    "    FROM test-project.test-dataset.orders" +
                    "  )" +
                    ")";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("order_id"));
        assertTrue(result.contains("amount"));
        assertTrue(result.contains("created_at"));
        
        assertFalse(result.contains("AVG(amount)"));
        assertFalse(result.contains("MAX(created_at)"));
    }

    @Test
    void testAnalyzeWithTableAliases() {
        String sql = "SELECT " +
                    "  u.name as user_name, " +
                    "  o.order_id as order_number, " +
                    "  p.name as product_name " +
                    "FROM test-project.test-dataset.users u " +
                    "JOIN test-project.test-dataset.orders o ON u.id = o.user_id " +
                    "JOIN test-project.test-dataset.products p ON o.product_id = p.id";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("test-project.test-dataset.products"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("order_id"));
        assertTrue(result.contains("id"));
    }

    @Test
    void testAnalyzeWithBackticks() {
        String sql = "SELECT `id`, `name` " +
                    "FROM `test-project`.`test-dataset`.`users` " +
                    "WHERE `created_at` > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("id"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("created_at"));
    }

    @Test
    void testAnalyzeWithAggregations() {
        String sql = "SELECT " +
                    "  u.name, " +
                    "  COUNT(DISTINCT o.order_id) as total_orders, " +
                    "  SUM(oi.quantity) as total_items, " +
                    "  AVG(oi.unit_price) as avg_price " +
                    "FROM test-project.test-dataset.users u " +
                    "JOIN test-project.test-dataset.orders o ON u.id = o.user_id " +
                    "JOIN test-project.test-dataset.order_items oi ON o.order_id = oi.order_id " +
                    "GROUP BY u.name";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("test-project.test-dataset.order_items"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("order_id"));
        assertTrue(result.contains("quantity"));
        assertTrue(result.contains("unit_price"));
    }

    @Test
    void testAnalyzeWithWindowFunctions() {
        String sql = "SELECT " +
                    "  u.name, " +
                    "  o.order_id, " +
                    "  o.amount, " +
                    "  ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY o.created_at DESC) as rn " +
                    "FROM test-project.test-dataset.users u " +
                    "JOIN test-project.test-dataset.orders o ON u.id = o.user_id";
        String result = sqlLineageAnalyzer.analyzeLineage(sql);
        
        assertNotNull(result);
        assertTrue(result.contains("test-project.test-dataset.users"));
        assertTrue(result.contains("test-project.test-dataset.orders"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("order_id"));
        assertTrue(result.contains("amount"));
        assertTrue(result.contains("created_at"));
    }
} 