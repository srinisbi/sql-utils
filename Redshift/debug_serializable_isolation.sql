/************************************************************************
 * Script to debug serialization issue
 
 * Example Error Msg: 
 	Detail: Serializable isolation violation on table - 56524446, 
 	transactions forming the cycle are: 178678355, 178676747, 178677946 (pid:14023)"
 * ***************************************************************/

-- find the transaction id of query which caused conflict
select * from stl_tr_conflict where xact_id in (306618126, 306618450, 306617707);

-- This query will dipaly the object name and the locks acuqired by all processes 
-- which caused the transaction conflict

WITH aborted_transactions as (
SELECT tc.xact_id as aborted_xid,
tc.table_id AS tbl,
tc.xact_start_ts transaction_start_time,
tc.abort_time transaction_abort_time
FROM
stl_tr_conflict tc
WHERE
tc.xact_id=306618126  /*result xid from first query*/
),
concurrent_transactions as (
SELECT at.aborted_xid as aborted_xid,
at.tbl as aborted_table,
s.xid as concurrent_xid,
min(s.starttime) as transaction_starttime,
max(s.endtime) as transaction_endtime
FROM aborted_transactions at,
svl_statementtext s
WHERE s.xid in (306618126, 306618450, 306617707) /* xids from the error meesage */
GROUP BY aborted_xid, aborted_table, concurrent_xid
),
--find all tables touched by these transactions
concurrent_operations as (
SELECT ct.aborted_xid, ct.concurrent_xid, d.query, q.starttime, q.endtime, d.tbl, 'D' as operation, q.aborted, substring(querytxt, 1, 50) querytxt 
FROM stl_delete d,stl_query q,concurrent_transactions ct WHERE d.query=q.query AND ct.concurrent_xid = q.xid
UNION
--insert, update, copy statements
SELECT ct.aborted_xid, ct.concurrent_xid, i.query, q.starttime, q.endtime, i.tbl, 'W' as operation, q.aborted, substring(querytxt, 1, 50) querytxt 
FROM stl_insert i,stl_query q,concurrent_transactions ct WHERE i.query=q.query AND ct.concurrent_xid = q.xid
UNION
--select statements
SELECT ct.aborted_xid, ct.concurrent_xid, s.query, q.starttime, q.endtime, s.tbl, 'R' as operation, q.aborted, substring(querytxt, 1, 50) querytxt 
FROM stl_scan s,stl_query q,concurrent_transactions ct WHERE s.type=2 AND s.query=q.query AND ct.concurrent_xid = q.xid
UNION
--maybe add aborted?
SELECT ct.aborted_xid, ct.concurrent_xid, q.query, q.starttime, q.endtime, stc.table_id as tbl, 'A' as operation, q.aborted, substring(querytxt, 1, 50) querytxt 
FROM stl_tr_conflict stc,stl_query q,concurrent_transactions ct WHERE stc.xact_id=ct.concurrent_xid AND q.xid=stc.xact_id and q.aborted=1
)
SELECT co.aborted_xid, co.concurrent_xid, co.query, co.starttime, co.endtime, co.tbl, tn."table", co.operation, co.aborted, co.querytxt 
FROM concurrent_operations co LEFT JOIN svv_table_info tn ON co.tbl=tn.table_id ORDER BY co.starttime;
