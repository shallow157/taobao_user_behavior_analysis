-- 获客情况分析
-- 单日PV、UV统计
SELECT dates,
       COUNT(*) 'pv',
       COUNT(DISTINCT user_id) 'uv',
       ROUND(COUNT(*)/COUNT(DISTINCT user_id), 1) 'pv/uv'
FROM user_behavior
WHERE behavior_type = 'pv'
GROUP BY dates;

-- 结果存储
CREATE TABLE pv_uv_puv (
    dates CHAR(10),
    pv INT(9),
    uv INT(9),
    puv DECIMAL(10, 1)
);
INSERT INTO pv_uv_puv
SELECT dates,
       COUNT(*) 'pv',
       COUNT(DISTINCT user_id) 'uv',
       ROUND(COUNT(*)/COUNT(DISTINCT user_id), 1) 'pv/uv'
FROM user_behavior
WHERE behavior_type = 'pv'
GROUP BY dates;

-- 留存情况分析
-- 留存率计算
CREATE TABLE retention_rate (
    dates CHAR(10),
    retention_1 FLOAT
);
INSERT INTO retention_rate
SELECT a.dates,
       COUNT(IF(DATEDIFF(b.dates, a.dates) = 1, b.user_id, NULL)) /
       COUNT(IF(DATEDIFF(b.dates, a.dates) = 0, b.user_id, NULL)) retention_1
FROM (
    SELECT user_id, dates
    FROM user_behavior
    GROUP BY user_id, dates
) a
JOIN (
    SELECT user_id, dates
    FROM user_behavior
    GROUP BY user_id, dates
) b ON a.user_id = b.user_id AND a.dates <= b.dates
GROUP BY a.dates;

-- 跳失率分析
-- 跳失用户数
SELECT COUNT(*) FROM (
    SELECT user_id FROM user_behavior
    GROUP BY user_id
    HAVING COUNT(behavior_type) = 1
) a;
-- 跳失率计算
-- 跳失率 = 跳失用户数 / 总PV数

-- 时间序列分析
CREATE TABLE date_hour_behavior (
    dates CHAR(10),
    hours CHAR(2),
    pv INT,
    cart INT,
    fav INT,
    buy INT
);
INSERT INTO date_hour_behavior
SELECT dates, hours,
       COUNT(IF(behavior_type = 'pv', behavior_type, NULL)) 'pv',
       COUNT(IF(behavior_type = 'cart', behavior_type, NULL)) 'cart',
       COUNT(IF(behavior_type = 'fav', behavior_type, NULL)) 'fav',
       COUNT(IF(behavior_type = 'buy', behavior_type, NULL)) 'buy'
FROM user_behavior
GROUP BY dates, hours
ORDER BY dates, hours;

-- 用户转化率分析
-- 各类行为用户数
CREATE TABLE behavior_user_num (
    behavior_type VARCHAR(5),
    user_num INT
);
INSERT INTO behavior_user_num
SELECT behavior_type, COUNT(DISTINCT user_id) user_num
FROM user_behavior
GROUP BY behavior_type
ORDER BY behavior_type DESC;
-- 购买率计算
SELECT 2015807/89660670; -- 购买行为数 / 总浏览数
-- 收藏加购率计算
SELECT (2888255+5530446)/89660670; -- (收藏数+加购数) / 总浏览数

-- 行为路径分析
-- 创建用户行为视图
CREATE VIEW user_behavior_view AS
SELECT user_id, item_id,
       COUNT(IF(behavior_type = 'pv', behavior_type, NULL)) 'pv',
       COUNT(IF(behavior_type = 'fav', behavior_type, NULL)) 'fav',
       COUNT(IF(behavior_type = 'cart', behavior_type, NULL)) 'cart',
       COUNT(IF(behavior_type = 'buy', behavior_type, NULL)) 'buy'
FROM user_behavior
GROUP BY user_id, item_id;
-- 行为标准化
CREATE VIEW user_behavior_standard AS
SELECT user_id, item_id,
       (CASE WHEN pv > 0 THEN 1 ELSE 0 END) 浏览了,
       (CASE WHEN fav > 0 THEN 1 ELSE 0 END) 收藏了,
       (CASE WHEN cart > 0 THEN 1 ELSE 0 END) 加购了,
       (CASE WHEN buy > 0 THEN 1 ELSE 0 END) 购买了
FROM user_behavior_view;
-- 路径类型生成
CREATE VIEW user_behavior_path AS
SELECT *,
       CONCAT(浏览了, 收藏了, 加购了, 购买了) 购买路径类型
FROM user_behavior_standard AS a
WHERE a.购买了 > 0;

-- RFM模型分析
-- 创建RFM模型表
CREATE TABLE rfm_model (
    user_id INT,
    frequency INT,
    recent CHAR(10)
);
INSERT INTO rfm_model
SELECT user_id,
       COUNT(user_id) '购买次数',
       MAX(dates) '最近购买时间'
FROM user_behavior
WHERE behavior_type = 'buy'
GROUP BY user_id
ORDER BY 2 DESC, 3 DESC;
-- 频率评分
ALTER TABLE rfm_model ADD COLUMN fscore INT;
UPDATE rfm_model
SET fscore = CASE
    WHEN frequency BETWEEN 100 AND 262 THEN 5
    WHEN frequency BETWEEN 50 AND 99 THEN 4
    WHEN frequency BETWEEN 20 AND 49 THEN 3
    WHEN frequency BETWEEN 5 AND 20 THEN 2
    ELSE 1
END;
-- 最近购买时间评分
ALTER TABLE rfm_model ADD COLUMN rscore INT;
UPDATE rfm_model
SET rscore = CASE
    WHEN recent = '2017-12-03' THEN 5
    WHEN recent IN ('2017-12-01', '2017-12-02') THEN 4
    WHEN recent IN ('2017-11-29', '2017-11-30') THEN 3
    WHEN recent IN ('2017-11-27', '2017-11-28') THEN 2
    ELSE 1
END;