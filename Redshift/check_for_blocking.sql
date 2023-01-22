/*
* To idnetify which query/pid is blocking a process, use PID for the query to filter
*/

SELECT
	a.txn_owner,
	a.txn_db,
	a.xid,
	a.pid,
	a.txn_start,
	a.lock_mode,
	a.relation AS table_id,
	nvl(trim(c."name"),
	d.relname) AS tablename,
	a.granted,
	b.pid AS blocking_pid ,
	datediff(s,
	a.txn_start,
	getdate())/ 86400 || ' days ' 
		|| datediff(s,a.txn_start,getdate())%86400 / 3600 || ' hrs ' 
		|| datediff(s,a.txn_start,getdate())%3600 / 60 || ' mins ' 
		|| datediff(s,a.txn_start,getdate())%60 || ' secs' AS txn_duration
FROM
	svv_transactions a
LEFT JOIN (
	SELECT
		pid,
		relation,
		GRANTED
	FROM
		pg_locks
	GROUP BY
		1,2,3) b 
ON
	a.relation = b.relation
	AND a.granted = 'f'
	AND b.granted = 't'
LEFT JOIN (
	SELECT
		*
	FROM
		stv_tbl_perm
	WHERE
		slice = 0) c 
ON
	a.relation = c.id
LEFT JOIN pg_class d ON
	a.relation = d.oid
WHERE
	a.relation IS NOT NULL
ORDER BY blocking_pid desc;
	
