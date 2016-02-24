-- ----------------------------
--  create tables of kateway manager
-- ----------------------------

DROP TABLE IF EXISTS `application`;
CREATE TABLE `application` (
  `AppId` bigint(18) NOT NULL AUTO_INCREMENT COMMENT '应用Id',
  `ApplicationName` varchar(64) NOT NULL DEFAULT '',
  `ApplicationPinyin` varchar(64) NOT NULL DEFAULT '',
  `ApplicationIntro` varchar(255) NOT NULL DEFAULT '' COMMENT '应用描述',
  `CateId` int(11) NOT NULL COMMENT '所属分类',
  `Cluster` varchar(255) NOT NULL DEFAULT '' COMMENT 'kafka组集群名称',
  `CreateById` bigint(18) NOT NULL DEFAULT '0',
  `CreateBy` varchar(64) NOT NULL DEFAULT '',
  `CreateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `Status` tinyint(2) NOT NULL COMMENT '状态：-1待审核|1有效|-2无效|2删除',
  `AppSecret` varchar(64) NOT NULL DEFAULT '',
  PRIMARY KEY (`AppId`),
  KEY `CateId` (`CateId`)
) ENGINE=InnoDB AUTO_INCREMENT=34 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `application_category`;
CREATE TABLE `application_category` (
  `CateId` int(11) NOT NULL AUTO_INCREMENT COMMENT '分类Id',
  `CateName` varchar(64) NOT NULL COMMENT '分类名称',
  `CatePinyin` varchar(64) NOT NULL COMMENT '分类拼音',
  `ParentId` int(11) NOT NULL COMMENT '上级分类',
  `CreateById` bigint(18) NOT NULL DEFAULT '0',
  `CreateBy` varchar(64) NOT NULL,
  `CreateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `Status` tinyint(2) NOT NULL DEFAULT '1' COMMENT '0删除1正常',
  PRIMARY KEY (`CateId`),
  KEY `ParentId` (`ParentId`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `topics`;
CREATE TABLE `topics` (
  `TopicId` bigint(18) NOT NULL AUTO_INCREMENT,
  `AppId` bigint(18) NOT NULL,
  `CategoryId` int(11) NOT NULL COMMENT 'APP分类ID',
  `TopicName` varchar(64) NOT NULL COMMENT '主题名称',
  `TopicIntro` varchar(255) NOT NULL COMMENT '主题描述',
  `IDC` varchar(255) NOT NULL COMMENT '主题产生地（数据中心）',
  `CreateById` bigint(18) NOT NULL DEFAULT '0',
  `CreateBy` varchar(64) NOT NULL,
  `CreateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `Status` tinyint(2) NOT NULL COMMENT '状态：正常|废弃',
  PRIMARY KEY (`TopicId`),
  KEY `AppId` (`AppId`),
  KEY `CategoryId` (`CategoryId`),
  KEY `TopicName` (`TopicName`)
) ENGINE=InnoDB AUTO_INCREMENT=89 DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `topics_subscriber`;
CREATE TABLE `topics_subscriber` (
  `TopicId` bigint(18) NOT NULL,
  `AppId` bigint(18) NOT NULL,
  `TopicName` varchar(64) NOT NULL COMMENT '主题名称',
  `IDC` int(11) NOT NULL,
  `Callback` varchar(255) NOT NULL DEFAULT '',
  `CreateById` bigint(18) NOT NULL DEFAULT '0',
  `CreateBy` varchar(64) NOT NULL,
  `CreateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `Status` tinyint(2) NOT NULL COMMENT '状态：1订阅|2取消订阅',
  PRIMARY KEY (`AppId`,`TopicId`),
  KEY `TopicName` (`TopicName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `topics_version`;
CREATE TABLE `topics_version` (
  `TopicId` bigint(18) NOT NULL,
  `VerId` int(11) NOT NULL COMMENT '主题版本ID',
  `Instance` tinytext NOT NULL COMMENT '主题消息实例',
  `InstanceIntro` tinytext NOT NULL COMMENT '实例描述',
  `CreateById` bigint(18) NOT NULL DEFAULT '0',
  `CreateBy` varchar(64) NOT NULL,
  `CreateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `Status` tinyint(2) NOT NULL COMMENT '状态（使用中|已停用）',
  PRIMARY KEY (`TopicId`,`VerId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `user`;
CREATE TABLE `user` (
  `UserId` bigint(18) NOT NULL AUTO_INCREMENT,
  `UserName` varchar(64) NOT NULL COMMENT 'CTX账号名称',
  `RealName` varchar(64) NOT NULL,
  `Mobile` varchar(32) NOT NULL,
  `Email` varchar(64) NOT NULL,
  `CreateById` bigint(18) NOT NULL DEFAULT '0',
  `CreateBy` varchar(64) NOT NULL,
  `CreateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `Status` tinyint(2) NOT NULL DEFAULT '1' COMMENT '正常',
  PRIMARY KEY (`UserId`),
  KEY `CtxName` (`UserName`)
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=utf8 COMMENT='用户信息';

DROP TABLE IF EXISTS `user_access`;
CREATE TABLE `user_access` (
  `UserId` bigint(18) NOT NULL,
  `Group` int(11) NOT NULL COMMENT '组（APP分类Id）',
  `Role` varchar(32) NOT NULL COMMENT '角色：admin(负责人)|developer(开发者)',
  `Apps` tinytext NOT NULL COMMENT '应用Id,多个逗号分隔',
  KEY `UserId` (`UserId`,`Role`,`Group`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户权限';

DROP TABLE IF EXISTS `user_role`;
CREATE TABLE `user_role` (
  `UserName` varchar(64) NOT NULL,
  `Role` varchar(32) NOT NULL,
  `Resource` bigint(20) NOT NULL COMMENT '资源',
  `ResourceType` varchar(32) NOT NULL DEFAULT '' COMMENT '资源类型：App|AppCategory',
  KEY `UserName` (`UserName`,`Role`,`ResourceType`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

