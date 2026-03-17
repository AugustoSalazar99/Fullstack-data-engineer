
-- Ventas totales, margen total, número de pedidos, ticket promedio
SELECT
    ROUND(SUM(revenue), 2)                            AS ventas_totales,
    ROUND(SUM(margin),  2)                            AS margen_total,
    COUNT(*)                                          AS num_pedidos,
    ROUND(SUM(revenue) / NULLIF(COUNT(*), 0), 2)      AS ticket_promedio
FROM consumption.fact_orders;

-- Ventas por mes
SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(f.revenue), 2) AS ventas,
    ROUND(SUM(f.margin),  2) AS margen,
    COUNT(*)                 AS pedidos
FROM consumption.fact_orders f
JOIN consumption.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Ventas por canal

SELECT
    channel,
    ROUND(SUM(revenue), 2)                                          AS ventas,
    COUNT(*)                                                        AS pedidos,
    ROUND(SUM(revenue) * 100.0 / SUM(SUM(revenue)) OVER (), 2)     AS pct_ventas
FROM consumption.fact_orders
GROUP BY channel
ORDER BY ventas DESC;

-- Margen por categoría de producto

SELECT
    p.category,
    ROUND(SUM(f.revenue), 2)                          AS ventas,
    ROUND(SUM(f.margin),  2)                          AS margen,
    ROUND(SUM(f.margin) * 100.0 / NULLIF(SUM(f.revenue), 0), 2) AS margen_pct
FROM consumption.fact_orders f
JOIN consumption.dim_product p ON f.sku = p.sku
GROUP BY p.category
ORDER BY margen DESC;


-- Top 10 clientes por ingresos

SELECT
    c.customer_id,
    c.name,
    c.country,
    c.segment,
    ROUND(SUM(f.revenue), 2) AS ventas_totales,
    COUNT(*)                 AS num_pedidos
FROM consumption.fact_orders f
JOIN consumption.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.country, c.segment
ORDER BY ventas_totales DESC
LIMIT 10;



-- Top 10 productos más vendidos 
SELECT
    p.sku,
    p.category,
    SUM(f.quantity)          AS unidades_vendidas,
    ROUND(SUM(f.revenue), 2) AS ventas_totales,
    ROUND(SUM(f.margin),  2) AS margen_total
FROM consumption.fact_orders f
JOIN consumption.dim_product p ON f.sku = p.sku
GROUP BY p.sku, p.category
ORDER BY ventas_totales DESC
LIMIT 10;


-- datos para importar solo una tabla para el dashboard de KPIs
CREATE OR REPLACE VIEW consumption.vw_fact_full AS
SELECT
    f.order_id,
    f.date_id       AS order_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    f.channel,
    f.quantity,
    f.unit_price,
    f.revenue,
    f.cost,
    f.margin,
    c.customer_id,
    c.name          AS customer_name,
    c.country,
    c.segment,
    p.sku,
    p.category,
    p.cost          AS product_cost,
    p.active        AS product_active
FROM consumption.fact_orders f
JOIN consumption.dim_date     d ON f.date_id     = d.date_id
JOIN consumption.dim_customer c ON f.customer_id = c.customer_id
JOIN consumption.dim_product  p ON f.sku         = p.sku;
