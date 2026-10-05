-- Q1 主键检索
SELECT * FROM ticket_current WHERE unique_key = ?;
-- Q2 机构活动工单稳定键分页
SELECT unique_key,created_date,complaint_type,status FROM ticket_current WHERE agency=? AND status=? AND (created_date,unique_key)<(?,?) ORDER BY created_date DESC,unique_key DESC LIMIT 100;
-- Q3 每日需求趋势
SELECT DATE(created_date) created_day,COUNT(*) ticket_count FROM ticket_current GROUP BY DATE(created_date) ORDER BY created_day;
-- Q4 问题类型 Top N
SELECT complaint_type,COUNT(*) ticket_count FROM ticket_current GROUP BY complaint_type ORDER BY ticket_count DESC LIMIT 20;
-- Q5 有效闭单时长
SELECT agency,AVG(TIMESTAMPDIFF(SECOND,created_date,closed_date)) avg_seconds FROM ticket_current WHERE status='Closed' AND closed_date>=created_date GROUP BY agency;
-- Q6 活动工单年龄
SELECT agency,COUNT(*) active_count,AVG(TIMESTAMPDIFF(HOUR,created_date,'2026-01-01')) avg_age_hours FROM ticket_current WHERE status<>'Closed' GROUP BY agency;
-- Q7 多业务版本工单
SELECT unique_key,COUNT(*) version_count FROM ticket_observation GROUP BY unique_key HAVING COUNT(*)>1;
-- Q8 发布后的机构/区域指标
SELECT agency,borough,status,ticket_count FROM ads_agency_status WHERE release_id=? ORDER BY ticket_count DESC;
