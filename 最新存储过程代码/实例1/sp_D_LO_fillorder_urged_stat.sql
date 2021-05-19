CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_fillorder_urged_stat`()
begin
INSERT INTO feods.D_LO_fillorder_urged_stat(
shelf_manager_id            
,shelf_id                          
,stat_date)
SELECT
        f.`MANAGER_ID`,
-- 	t.`user_id`,
	t.`shelf_id`,
	t.`add_time`
FROM
	fe_cms.csm_product_vote_submit t
JOIN fe_cms.csm_product_vote e 
ON t.vote_id = e.vote_id
JOIN fe.`sf_shelf` f
ON t.`shelf_id` = f.`SHELF_ID`
WHERE e.vote_type = 3
AND t.`data_flag` = 1
AND e.`data_flag` =1
AND f.`DATA_FLAG` = 1 ;
SELECT
        f.`MANAGER_ID`,
	t.`user_id`,
	t.`shelf_id`,
	m.`product_id`,
	t.`add_time`
FROM
fe_cms.csm_product_vote_submit_detail m
JOIN
fe_cms.csm_product_vote_submit t
ON m.`submit_id` = t.`submit_id`
JOIN fe_cms.csm_product_vote e 
ON t.vote_id = e.vote_id
JOIN fe.`sf_shelf` f
ON t.`shelf_id` = f.`SHELF_ID`
WHERE e.vote_type = 3
AND t.`data_flag` = 1
AND e.`data_flag` =1
AND f.`DATA_FLAG` = 1
AND m.`data_flag` = 1;
end