CREATE TABLE Job (
    uid bigint unsigned NOT NULL DEFAULT 0,
    job_id bigint unsigned NOT NULL DEFAULT 0 COMMENT "event ticket",
    event_type int unsigned NOT NULL DEFAULT 0 COMMENT "event type, describe by constant defined in EventModel",
    time_start timestamp NOT NULL DEFAULT "0000-00-00 00:00:00" COMMENT "start time point of the event",
    time_end timestamp NULL DEFAULT NULL COMMENT "end time point of the event",
    payload blob,
    ctime timestamp NOT NULL DEFAULT 0,
    mtime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (uid, job_id),
    KEY(time_end)
) ENGINE = INNODB DEFAULT CHARSET utf8;

