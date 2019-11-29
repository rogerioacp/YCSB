\set autocommit off;
--For each BEGIN the backend server might start a new memory context which 
-- deletes some of the information stored on the oblivpg_ftw such as the 
--global variables of the table name and index on the ocalls files

BEGIN;
select open_enclave();
select init_soe(0, CAST( get_ftw_oid() as INTEGER), 1, CAST (get_original_index_oid() as INTEGER));
select load_blocks(CAST (get_original_index_oid() as INTEGER), CAST (get_original_heap_oid() as INTEGER));
DECLARE tcursor CURSOR FOR select YCSB_KEY,YCSB_VALUE from ftw_usertable where YCSB_KEY='466089117';
FETCH NEXT IN tcursor;
CLOSE tcursor;
--COMMIT;
--END;
--BEGIN;
DECLARE tcursor CURSOR FOR select YCSB_KEY,YCSB_VALUE from ftw_usertable where YCSB_KEY='650165498';
FETCH NEXT IN tcursor;
CLOSE tcursor;
--END;
--COMMIT;
--BEGIN;
DECLARE tcursor CURSOR FOR select YCSB_KEY,YCSB_VALUE from ftw_usertable where YCSB_KEY='279093984';
FETCH NEXT IN tcursor;
CLOSE tcursor;
--COMMIT;
--END;
--BEGIN;
DECLARE tcursor CURSOR FOR select YCSB_KEY,YCSB_VALUE from ftw_usertable where YCSB_KEY='473967685';
FETCH NEXT IN tcursor;
CLOSE tcursor;
--COMMIT;
--END;
--BEGIN;
DECLARE tcursor CURSOR FOR select YCSB_KEY,YCSB_VALUE from ftw_usertable where YCSB_KEY='400245136';
FETCH NEXT IN tcursor;
CLOSE tcursor;
COMMIT;
--END;
