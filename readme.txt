local pg

= batchedInsert;  numRows = 22000000; batchSize = 100000 =
total	174862ms	100.0%
setObject	3488ms	2.0%
addBatch	2383ms	1.4%
executeBatch	167819ms	96.0%
commit	12ms	0.0%
126436.8 rows/sec

= copyInsert;  numRows = 22000000; batchSize = 100000 =
total	33713ms	100.0%
export	1510ms	4.5%
writeToCopy	345ms	1.0%
endCopy	31745ms	94.2%
commit	45ms	0.1%
666666.7 rows/sec

