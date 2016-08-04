
CREATE TABLE AppLookup (
    entityId bigint unsigned NOT NULL DEFAULT 0,
    shardId mediumint unsigned NOT NULL DEFAULT 0,
    name varchar(64) NOT NULL DEFAULT "",
    shardLock tinyint unsigned NOT NULL DEFAULT 0,
    ctime timestamp NOT NULL DEFAULT 0,
    mtime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (entityId)
) ENGINE = INNODB DEFAULT CHARSET=utf8;

INSERT INTO AppLookup(entityId, shardId, name, shardLock, ctime) VALUES(65601907, 1, "app1", 0, now());

CREATE TABLE app1_foobar_v1 (
    app_id bigint unsigned NOT NULL DEFAULT 0,
    job_id bigint unsigned NOT NULL DEFAULT 0 COMMENT "",
    payload blob,
    ctime timestamp NOT NULL DEFAULT 0,
    mtime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    due_time timestamp NULL DEFAULT NULL COMMENT "end time point of the event",
    PRIMARY KEY (app_id, job_id),
    KEY(due_time)
) ENGINE = INNODB DEFAULT CHARSET utf8;

