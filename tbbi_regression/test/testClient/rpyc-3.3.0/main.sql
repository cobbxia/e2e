insert overwrite table m_cm_auction_category
select
cat.cat_id,
cat.cat_name,
case cat.level when 7 then if(cat.p7 = '0', cat.p5, null)
when 6 then if(cat.p6 = '0', cat.p4, null)
when 5 then if(cat.p5 = '0', cat.p3, null)
when 4 then if(cat.p4 = '0', cat.p2, null)
when 3 then if(cat.p3 = '0', cat.p1, null)
when 2 then if(cat.p2 = '0', cat.cat_id, null)
end as cm_level1,
case cat.level when 7 then cat.p4
when 6 then cat.p3
when 5 then cat.p2
when 4 then cat.p1
when 3 then cat.cat_id
end as cm_level2,
case cat.level when 7 then cat.p3
when 6 then cat.p2
when 5 then cat.p1
when 4 then cat.cat_id
end as cm_level3,
case cat.level when 7 then cat.p2
when 6 then cat.p1
when 5 then cat.cat_id
end as cm_level4,
case cat.level when 7 then cat.p1
when 6 then cat.cat_id
end as cm_level5,
case cat.level when 7 then if(cat.p7 = '0', cat.p5_name, null)
when 6 then if(cat.p6 = '0', cat.p4_name, null)
when 5 then if(cat.p5 = '0', cat.p3_name, null)
when 4 then if(cat.p4 = '0', cat.p2_name, null)
when 3 then if(cat.p3 = '0', cat.p1_name, null)
when 2 then if(cat.p2 = '0', cat.cat_name, null)
end as cm_name1,
case cat.level when 7 then cat.p4_name
when 6 then cat.p3_name
when 5 then cat.p2_name
when 4 then cat.p1_name
when 3 then cat.cat_name
end as cm_name2,
case cat.level when 7 then cat.p3_name
when 6 then cat.p2_name
when 5 then cat.p1_name
when 4 then cat.cat_name
end as cm_name3,
case cat.level when 7 then cat.p2_name
when 6 then cat.p1_name
when 5 then cat.cat_name
end as cm_name4,
case cat.level when 7 then cat.p1_name
when 6 then cat.cat_name
end as cm_name5
from (
select
cat.cat_id,
cat.cat_name,
p7.cat_id as p7,
p6.cat_id as p6,
p5.cat_id as p5,
p4.cat_id as p4,
p3.cat_id as p3,
p2.cat_id as p2,
p1.cat_id as p1,
p7.cat_name as p7_name,
p6.cat_name as p6_name,
p5.cat_name as p5_name,
p4.cat_name as p4_name,
p3.cat_name as p3_name,
p2.cat_name as p2_name,
p1.cat_name as p1_name,
case
when p7.cat_id is not null then 7
when p6.cat_id is not null then 6
when p5.cat_id is not null then 5
when p4.cat_id is not null then 4
when p3.cat_id is not null then 3
when p2.cat_id is not null then 2
when p1.cat_id is not null then 1
end as level
from (select * from s_cm_categories where pt='${date}000000') cat
left outer join (select * from s_cm_categories where pt='${date}000000') p1
on p1.cat_id=cat.parent_id
left outer join (select * from s_cm_categories where pt='${date}000000') p2
on p2.cat_id=p1.parent_id
left outer join (select * from s_cm_categories where pt='${date}000000') p3
on p3.cat_id=p2.parent_id
left outer join (select * from s_cm_categories where pt='${date}000000') p4
on p4.cat_id=p3.parent_id
left outer join (select * from s_cm_categories where pt='${date}000000') p5
on p5.cat_id=p4.parent_id
left outer join (select * from s_cm_categories where pt='${date}000000') p6
on p6.cat_id=p5.parent_id
left outer join (select * from s_cm_categories where pt='${date}000000') p7
on p7.cat_id=p6.parent_id
) cat
;
insert overwrite table r_cm_auction_category
partition (pt='${date}000000')
select * from
(
select
'${date}' as data_date
,a.cat_id as cm_cat_id
,a.parent_id as cm_parent_id
,a.cat_name as cm_cat_name
,cast(a.status as string) as status
,a.short_name
,a.cat_name_path
,a.related_forum_id
, bi_udf:bi_date_format( bi_udf:bi_substr(a.gmt_modified,0,10),'yyyy-MM-dd','yyyyMMdd') as gmt_modified
,a.channel
, bi_udf:bi_date_format( bi_udf:bi_substr(a.gmt_create,0,10),'yyyy-MM-dd','yyyyMMdd') as gmt_create
,a.sort_role
,cast(a.sort_order as string) as sort_order
,a.memo
,cast(a.feature_cc as string) as feature_cc
,cast(a.main_map as string) as main_map
,a.conditions
,cast(a.auction_count as string) as auction_count
,cast(a.highlight as string) as highlight
,b.cm_level1
,b.cm_level2
,b.cm_level3
,b.cm_name1
,b.cm_name2
,b.cm_name3
,cast(a.cat_type as string) as cat_type
,b.cm_level4
,b.cm_level5
,b.cm_name4
,b.cm_name5
,'' as features
from (select * from s_cm_categories where pt='20141221000000' and cat_id<>0 and parent_id<>'-1') a
left outer join (select * from m_cm_auction_category where cm_level1 is not null) b
on a.cat_id=b.cat_id
union all
select
'${date}' as data_date
,'-1' as cm_cat_id
,'0' as cm_parent_id
,'无类目' as cm_cat_name
,'0' as status
,'' as short_name
,'' as cat_name_path
,'0' as related_forum_id
,'${date}' as gmt_modified
,'' as channel
,'${date}' as gmt_create
,'' as sort_role
,'' as sort_order
,'' as memo
,'' as feature_cc
,'' as main_map
,'' as conditions
,'' as auction_count
,'' as highlight
,'0' as cm_level1
,'-1' as cm_level2
,'' as cm_level3
,'无类目' as cm_name1
,'无类目' as cm_name2
,'' as cm_name3
,'' as cat_type
,'' as cm_level4
,'' as cm_level5
,'' as cm_name4
,'' as cm_name5
,'' as features
from m_cm_auction_category limit 1
union all
select
'${date}' as data_date
,'0' as cm_cat_id
,'0' as cm_parent_id
,'无类目' as cm_cat_name
,'0' as status
,'' as short_name
,'' as cat_name_path
,'0' as related_forum_id
,'${date}' as gmt_modified
,'' as channel
,'${date}' as gmt_create
,'' as sort_role
,'' as sort_order
,'' as memo
,'' as feature_cc
,'' as main_map
,'' as conditions
,'' as auction_count
,'' as highlight
,'0' as cm_level1
,'0' as cm_level2
,'' as cm_level3
,'无类目' as cm_name1
,'无类目' as cm_name2
,'' as cm_name3
,'' as cat_type
,'' as cm_level4
,'' as cm_level5
,'' as cm_name4
,'' as cm_name5
,'' as features
from m_cm_auction_category limit 1
) result
;

