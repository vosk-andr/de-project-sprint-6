with user_group_log as (
	SELECT	tri.hk_group_id, COUNT(DISTINCT tri.hk_user_id) cnt_added_users 
	from (
		SELECT DISTINCT  luga.hk_group_id,  luga.hk_user_id
		from STV2025041935__DWH.s_auth_history sah
		left join STV2025041935__DWH.l_user_group_activity luga 
		using (hk_l_user_group_activity)
		WHERE sah.event = 'add') tri 
	left JOIN STV2025041935__DWH.h_groups hg using (hk_group_id)
	group by tri.hk_group_id, hg.registration_dt
	ORDER by  hg.registration_dt DESC limit 10
	),
user_group_messages as (
	SELECT 
		hk_group_id, 
		COUNT(DISTINCT hk_user_id) cnt_users_in_group_with_messages
	from(
    	select 
    		lgd.hk_group_id,
    		lum.hk_user_id
    	from STV2025041935__DWH.l_groups_dialogs lgd
    	left join STV2025041935__DWH.l_user_message lum using(hk_message_id)
    	group by lgd.hk_group_id, lum.hk_user_id
    	HAVING COUNT(lgd.hk_message_id) > 1
		) tri
	group BY hk_group_id)
select 
	ugl.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users) group_conversion
from user_group_log as ugl
left join user_group_messages as ugm using (hk_group_id)
order by group_conversion desc 
