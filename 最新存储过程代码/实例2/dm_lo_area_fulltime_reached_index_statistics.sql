CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_lo_area_fulltime_reached_index_statistics`()
begin
  -- =============================================
-- author:	物流店主
-- create date: 2019/09/20
-- modify date: 
-- description:	
--    更新区域维度全职达成指标统计结果表（每天的1时41分）
-- 
-- =============================================
  set @run_date := current_date(),@user := current_user(),@stime := current_timestamp();
  set @run_date := current_date();
  set @user := current_user();
  set @timestamp := current_timestamp();
  
  set @date_top:= date_add(
              date_sub(curdate(), interval 1 day),
              interval - day(date_sub(curdate(), interval 1 day)) + 1 day
            );
  set @date_end:= date_add(
              last_day(date_sub(curdate(), interval 1 day)),
              interval 1 day
            );
            
  delete
  from
    fe_dm.dm_lo_area_fulltime_reached_index_statistics
  where stat_date = date_format(
      date_sub(current_date, interval 1 day),
      '%y%m'
    );
insert into fe_dm.dm_lo_area_fulltime_reached_index_statistics (
      stat_date,
      region_area,
      business_area,
      shelf_type,
      manager_num,
      shelf_qty,
      area_shelf_qty,
      fulltime_gmv,
      area_gmv,
      revoked_shelf_qty,
      area_revoked_qty,
      low_stock_shelf_qty,
      area_low_stock_qty,
      stockout_rate,
      two_days_fill_rate
    )
select
  date_format(
    date_sub(current_date, interval 1 day),
    '%y%m'
  ) as '统计月份',
  a.`region_name` as '大区',
  a.business_name as '地区',
  a.shelf_type as '货架类型',
  count(
    distinct if(
      a.manager_type = '全职店主',
      a.manager_id,
      null
    )
  ) as '全职店主人数',
  count(
    distinct if(
      a.manager_type = '全职店主',
      a.shelf_id,
      null
    )
  ) as '全职店主货架数',
  count(distinct a.shelf_id) as '地区货架数',
  sum(
    if(
      a.manager_type = '全职店主',
      c.gmv,
      0
    )
  ) as '全职gmv',
  sum(c.gmv) as '地区gmv',
  count(
    distinct if(
      a.manager_type = '全职店主'
      and a.`shelf_status` = 3,
      a.shelf_id,
      null
    )
  ) as '全职撤架数',
  count(
    distinct if(
      a.`shelf_status` = 3,
      a.shelf_id,
      null
    )
  ) as '地区撤架数',
  count(
    distinct if(
      a.manager_type = '全职店主',
      case
        when j.package_model = 3
        and a.shelf_status = 2
        and a.grade in ('甲', '乙')
        and i.shelf_stock < 290
        then a.shelf_id
        when j.package_model = 4
        and a.shelf_status = 2
        and a.grade in ('甲', '乙')
        and i.shelf_stock < 360
        then a.shelf_id
        when j.package_model = 5
        and a.shelf_status = 2
        and a.grade in ('甲', '乙')
        and i.shelf_stock < 470
        then a.shelf_id
        when a.shelf_type in (1, 3)
        and a.shelf_status = 2
        and a.grade in ('甲', '乙')
        and i.shelf_stock < 180
        then a.shelf_id
        when a.shelf_type in (2, 5)
        and a.shelf_status = 2
        and a.grade in ('甲', '乙')
        and i.shelf_stock < 110
        then a.shelf_id
        when j.package_model = 3
        and a.shelf_status = 2
        and a.grade in ('丙', '丁')
        and i.shelf_stock < 200
        then a.shelf_id
        when j.package_model = 4
        and a.shelf_status = 2
        and a.grade in ('丙', '丁')
        and i.shelf_stock < 220
        then a.shelf_id
        when j.package_model = 5
        and a.shelf_status = 2
        and a.grade in ('丙', '丁')
        and i.shelf_stock < 310
        then a.shelf_id
        when a.shelf_type in (1, 3)
        and a.shelf_status = 2
        and a.grade in ('丙', '丁')
        and i.shelf_stock < 110
        then a.shelf_id
        when a.shelf_type in (2, 5)
        and a.shelf_status = 2
        and a.grade in ('丙', '丁')
        and i.shelf_stock < 90
        then a.shelf_id
      end,
      null
    )
  ) as '全职低库存货架数',
  count(
    distinct
    case
      when j.package_model = 3
      and a.shelf_status = 2
      and a.grade in ('甲', '乙')
      and i.shelf_stock < 290
      then a.shelf_id
      when j.package_model = 4
      and a.shelf_status = 2
      and a.grade in ('甲', '乙')
      and i.shelf_stock < 360
      then a.shelf_id
      when j.package_model = 5
      and a.shelf_status = 2
      and a.grade in ('甲', '乙')
      and i.shelf_stock < 470
      then a.shelf_id
      when a.shelf_type in (1, 3)
      and a.shelf_status = 2
      and a.grade in ('甲', '乙')
      and i.shelf_stock < 180
      then a.shelf_id
      when a.shelf_type in (2, 5)
      and a.shelf_status = 2
      and a.grade in ('甲', '乙')
      and i.shelf_stock < 110
      then a.shelf_id
      when j.package_model = 3
      and a.shelf_status = 2
      and a.grade in ('丙', '丁')
      and i.shelf_stock < 200
      then a.shelf_id
      when j.package_model = 4
      and a.shelf_status = 2
      and a.grade in ('丙', '丁')
      and i.shelf_stock < 220
      then a.shelf_id
      when j.package_model = 5
      and a.shelf_status = 2
      and a.grade in ('丙', '丁')
      and i.shelf_stock < 310
      then a.shelf_id
      when a.shelf_type in (1, 3)
      and a.shelf_status = 2
      and a.grade in ('丙', '丁')
      and i.shelf_stock < 110
      then a.shelf_id
      when a.shelf_type in (2, 5)
      and a.shelf_status = 2
      and a.grade in ('丙', '丁')
      and i.shelf_stock < 90
      then a.shelf_id
    end
  ) as '地区低库存货架数',
  sum(st.ifsto_num) / sum(st.ct) stockout_rate,
  sum(fm.in_num) / sum(fm.total_num) two_days_fill_rate
from
  fe_dwd.`dwd_shelf_base_day_all` a
   left join
    (select
      s.shelf_id,
      sum(s.gmv) as gmv
    from
      (select
        f.shelf_id,
        f.`order_id`,
        f.`pay_amount` * count(distinct f.pay_id) - sum(ifnull(f.refund_amount, 0)) as amount,
        sum(
          if(
            f.refund_amount > 0,
            f.quantity_act,
            f.`quantity`
          ) * f.`sale_price`
        ) as gmv
      from
        fe_dwd.`dwd_order_item_refund_day` f
      where f.pay_date >= @date_top
        and f.pay_date < @date_end
      group by f.`order_id`) s
    group by s.shelf_id) c -- 销售数据
     on a.shelf_id = c.shelf_id
  left join
    (select
      date_format(t.`sdate`, '%y%m') smonth,
      t.`shelf_id`,
      sum(if(t.`ifsto` = 0, t.`ct`, 0)) ifsto_num,
      sum(t.`ct`) ct
    from
      fe_dm.dm_op_s_offstock t force index (sdate)
    where t.`sdate` >= @date_top
      and t.`sdate` < @date_end
    group by t.`shelf_id`) st -- 缺货率指标
     on a.shelf_id = st.shelf_id
  left join
    (select
      date_format(t.apply_time, '%y%m') smonth,
      t.`shelf_id`,
      count(
        distinct if(
          t.`two_days_fill_label` = '及时',
          t.`order_id`,
          null
        )
      ) in_num,
      count(distinct t.order_id) total_num
    from
      fe_dm.dm_lo_shelf_fill_timeliness_detail t
    where t.`apply_time` >= @date_top
      and t.`apply_time` < @date_end
    group by t.`shelf_id`) fm -- 补货次日上架率指标
     on a.shelf_id = fm.shelf_id
  left join
    (select
      s.`shelf_id`,
      sum(s.`stock_quantity`) as shelf_stock
    from
      fe_dwd.`dwd_shelf_product_sto_sal_30_days` s
    where s.`sdate` = subdate(current_date, 1)
    group by s.`shelf_id`) i            -- 昨日的货架期末库存结余数据
     on a.shelf_id = i.shelf_id
  left join
    (select
      a.main_shelf_id,
      max(a.package_model) as package_model
    from
      fe_dwd.`dwd_sf_shelf_relation_record` a
    where a.data_flag = 1
      and a.shelf_handle_status = 9
    group by a.main_shelf_id) j -- 关联货架数据
     on a.shelf_id = j.main_shelf_id
where a.shelf_status in (2, 5)
  or (
    a.revoke_time >= @date_top
    and a.revoke_time < @date_end
  )
group by a.business_name,
  a.shelf_type;
  
  -- 计算更新全职货架覆盖率
  update
    fe_dm.dm_lo_area_fulltime_reached_index_statistics t
  set
    t.shelf_coverage_rate = round(t.shelf_qty / t.area_shelf_qty, 2)
  where t.shelf_qty is not null
    and t.area_shelf_qty is not null
    and t.area_shelf_qty != 0;
  -- 计算更新全职gmv覆盖率
  update
    fe_dm.dm_lo_area_fulltime_reached_index_statistics t
  set
    t.gmv_coverage_rate = round(t.fulltime_gmv / t.area_gmv, 2)
  where t.fulltime_gmv is not null
    and t.area_gmv is not null
    and t.area_gmv != 0;
-- 执行记录日志
call sh_process.`sp_sf_dw_task_log` (
'dm_lo_area_fulltime_reached_index_statistics',
date_format(@run_date, '%Y-%m-%d'),
concat('蔡松林@', @user),
@stime);
-- 记录表的数据量
-- call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_lo_area_fulltime_reached_index_statistics','dm_lo_area_fulltime_reached_index_statistics','蔡松林');
end