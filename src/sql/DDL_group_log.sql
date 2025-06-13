
DROP TABLE IF EXISTS STV2025041935__STAGING.group_log CASCADE;

-- Создание таблицы group_log
CREATE TABLE STV2025041935__STAGING.group_log  (
    group_id  INT NOT NULL,
    user_id   INT NOT NULL,
    user_id_from INT,
    event VARCHAR(10) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    CONSTRAINT fk_from FOREIGN KEY (user_id_from) REFERENCES STV2025041935__STAGING.users(id),
    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES STV2025041935__STAGING.users(id),
    CONSTRAINT fk_group FOREIGN KEY (group_id) REFERENCES STV2025041935__STAGING.groups(id))
ORDER BY group_id, datetime
SEGMENTED BY HASH(group_id) ALL NODES
PARTITION BY datetime::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(datetime::DATE, 3, 2);
