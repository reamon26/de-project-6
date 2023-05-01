drop table RINATSHAKBASAROVYANDEXRU__STAGING.dialogs;
create table RINATSHAKBASAROVYANDEXRU__STAGING.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(9),
    message_from int,
    message_to   int,
    message      varchar(1000),
    -- message_group int,
    message_type int
)
ORDER BY message_id
--segmented by hash(message_id) all nodes
partition by message_ts::date
group by calendar_hierarchy_day(message_ts::date, 3, 2);

drop table RINATSHAKBASAROVYANDEXRU__STAGING.groups;
create table RINATSHAKBASAROVYANDEXRU__STAGING.groups
(
    id                  int PRIMARY KEY,
    admin_id            int,
    group_name          varchar (100),
    registration_dt     timestamp(6),
    is_private          boolean
)
order by  id, admin_id
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);

drop table RINATSHAKBASAROVYANDEXRU__STAGING.users;
create table RINATSHAKBASAROVYANDEXRU__STAGING.users
(
    id                  int PRIMARY KEY,
    chat_name           varchar (200),
    registration_dt     timestamp,
    country             varchar (200),
    age                 int
)
order by id;