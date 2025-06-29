CREATE DATABASE taobao;
USE taobao;
CREATE TABLE user_behavior (
    user_id INT(9),
    item_id INT(9),
    category_id INT(9),
    behavior_type VARCHAR(5),
    timestamp INT(14)
);