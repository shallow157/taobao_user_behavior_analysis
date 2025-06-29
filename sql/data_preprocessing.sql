-- 字段重命名
ALTER TABLE user_behavior CHANGE timestamp timestamps INT(14);

-- 空值检查
SELECT * FROM user_behavior WHERE user_id IS NULL;
-- 重复值检查
SELECT user_id, item_id, timestamps FROM user_behavior
GROUP BY user_id, item_id, timestamps
HAVING COUNT(*) > 1;

-- 去重处理
ALTER TABLE user_behavior ADD id INT FIRST;
ALTER TABLE user_behavior MODIFY id INT PRIMARY KEY AUTO_INCREMENT;
DELETE u FROM user_behavior u
JOIN (
    SELECT user_id, item_id, timestamps, MIN(id) id
    FROM user_behavior
    GROUP BY user_id, item_id, timestamps
    HAVING COUNT(*) > 1
) t2 ON u.user_id = t2.user_id 
AND u.item_id = t2.item_id 
AND u.timestamps = t2.timestamps 
AND u.id > t2.id;

-- 时间维度处理
-- 转换时间戳为日期时间
ALTER TABLE user_behavior ADD datetimes TIMESTAMP(0);
UPDATE user_behavior SET datetimes = FROM_UNIXTIME(timestamps);
-- 提取日期、时间、小时
ALTER TABLE user_behavior ADD dates CHAR(10);
ALTER TABLE user_behavior ADD times CHAR(8);
ALTER TABLE user_behavior ADD hours CHAR(2);
UPDATE user_behavior SET dates = SUBSTRING(datetimes, 1, 10);
UPDATE user_behavior SET times = SUBSTRING(datetimes, 12, 8);
UPDATE user_behavior SET hours = SUBSTRING(datetimes, 12, 2);
-- 过滤异常时间数据
DELETE FROM user_behavior
WHERE datetimes < '2017-11-25 00:00:00'
OR datetimes > '2017-12-03 23:59:59';