drop table if exists RINATSHAKBASAROVYANDEXRU__STAGING.group_log;
create table RINATSHAKBASAROVYANDEXRU__STAGING.group_log
(
    group_id   int PRIMARY KEY,
    user_id   int,
    user_id_from int,
    event   varchar(100),
    datetime      timestamp
)
ORDER BY group_id
partition by datetime::date
group by calendar_hierarchy_day(datetime::date, 3, 2);

drop table if exists RINATSHAKBASAROVYANDEXRU__DWH.l_user_group_activity cascade ;
create table RINATSHAKBASAROVYANDEXRU__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id      bigint not null CONSTRAINT fk_l_user_group_activity_h_users REFERENCES RINATSHAKBASAROVYANDEXRU__DWH.h_users  (hk_user_id),
hk_group_id     bigint not null CONSTRAINT fk_l_user_group_activity_h_groups REFERENCES RINATSHAKBASAROVYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO RINATSHAKBASAROVYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
hash(hg.hk_group_id,hu.hk_user_id) as hk_l_user_group_activity,
hu.hk_user_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from RINATSHAKBASAROVYANDEXRU__STAGING.group_log as gl
left join RINATSHAKBASAROVYANDEXRU__DWH.h_users hu on gl.user_id= hu.user_id
left join RINATSHAKBASAROVYANDEXRU__DWH.h_groups hg on gl.group_id = hg.group_id;

drop table if exists RINATSHAKBASAROVYANDEXRU__DWH.s_auth_history;
create table RINATSHAKBASAROVYANDEXRU__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES RINATSHAKBASAROVYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(20),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY user_id_from all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO RINATSHAKBASAROVYANDEXRU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event, event_dt,load_dt,load_src)
select
luga.hk_l_user_group_activity,
gl.user_id_from as user_id_from,
gl.event as event,
gl.datetime as event_dt,
now() as load_dt,
's3' as load_src
from RINATSHAKBASAROVYANDEXRU__DWH.l_user_group_activity as luga
left join RINATSHAKBASAROVYANDEXRU__DWH.h_users as hu on luga.hk_user_id = hu.hk_user_id
left join RINATSHAKBASAROVYANDEXRU__DWH.h_groups as hg on luga.hk_group_id = hg.hk_group_id
left join RINATSHAKBASAROVYANDEXRU__STAGING.group_log as gl on hu.user_id = gl.user_id and hg.group_id=gl.group_id;