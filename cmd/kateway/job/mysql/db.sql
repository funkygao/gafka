
CREATE TABLE IF NOT EXISTS AppLookup (
    entityId bigint unsigned NOT NULL DEFAULT 0,
    shardId mediumint unsigned NOT NULL DEFAULT 0,
    name varchar(64) NOT NULL DEFAULT "",
    shardLock tinyint unsigned NOT NULL DEFAULT 0,
    ctime timestamp NOT NULL DEFAULT 0,
    mtime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (entityId)
) ENGINE = INNODB DEFAULT CHARSET=utf8;

INSERT IGNORE INTO AppLookup(entityId, shardId, name, shardLock, ctime) VALUES(65601907, 1, "app1", 0, now());

