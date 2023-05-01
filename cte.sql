
-- # Шаг 7.1. Подготовить CTE user_group_messages
with user_group_messages as (
    select hg.hk_group_id, count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from RINATSHAKBASAROVYANDEXRU__DWH.h_groups hg
        join RINATSHAKBASAROVYANDEXRU__DWH.l_groups_dialogs  lgd on hg.hk_group_id = lgd.hk_group_id
        join RINATSHAKBASAROVYANDEXRU__DWH.h_dialogs hd on lgd.hk_message_id = hd.hk_message_id
        join RINATSHAKBASAROVYANDEXRU__DWH.l_user_message lum on hd.hk_message_id = lum.hk_message_id
    group by 1
)
select hk_group_id, cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages desc
limit 10;


-- # Шаг 7.2. Подготовить CTE user_group_log
with user_group_log as (
    select luga.hk_group_id,
           registration_dt,
           count(distinct luga.hk_user_id) as cnt_added_users
    from RINATSHAKBASAROVYANDEXRU__DWH.l_user_group_activity luga
        join RINATSHAKBASAROVYANDEXRU__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
        join RINATSHAKBASAROVYANDEXRU__DWH.h_groups hg on luga.hk_group_id = hg.hk_group_id
    where event='add'
    group by 1,2
    order by 2
    limit 10
)
select hk_group_id ,cnt_added_users
from user_group_log
order by cnt_added_users;



-- # Шаг 7.3. Написать запрос и ответить на вопрос бизнеса
with user_group_messages as (
    select hg.hk_group_id as hk_group_id,
           count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from RINATSHAKBASAROVYANDEXRU__DWH.h_groups hg
        join RINATSHAKBASAROVYANDEXRU__DWH.l_groups_dialogs  lgd on hg.hk_group_id = lgd.hk_group_id
        join RINATSHAKBASAROVYANDEXRU__DWH.h_dialogs hd on lgd.hk_message_id = hd.hk_message_id
        join RINATSHAKBASAROVYANDEXRU__DWH.l_user_message lum on hd.hk_message_id = lum.hk_message_id
    group by 1
),
user_group_log as (
select luga.hk_group_id as hk_group_id,
       registration_dt,
       count(distinct luga.hk_user_id) as cnt_added_users
from RINATSHAKBASAROVYANDEXRU__DWH.l_user_group_activity luga
    join RINATSHAKBASAROVYANDEXRU__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    join RINATSHAKBASAROVYANDEXRU__DWH.h_groups hg on luga.hk_group_id = hg.hk_group_id
where event='add'
group by 1,2
order by 2
)

select ugl.hk_group_id, cnt_users_in_group_with_messages, cnt_added_users, cnt_users_in_group_with_messages / cnt_added_users as group_conversion
from user_group_log ugl join user_group_messages ugm on ugl.hk_group_id=ugm.hk_group_id
order by 4 desc