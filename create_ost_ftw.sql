CREATE EXTENSION oblivpg_fdw;


create table cfpb(
	datereceived char(10),
	product varchar(30),
	subproduct varchar(30),
	issue varchar(50),
	subissue varchar(40),
	consumercomp varchar(30),
	comapnypresp varchar(50),
	company varchar(50),
	state varchar(9),
	zip varchar(5),
	tags varchar(20),
	consumer varchar(20),
	submitted varchar(10),
	datesent char(10),
	compres char(30),
	timerep char(3),
	consdisp char(3),
	compid  char(7) 
);

create index cfpb_date on cfpb using btree (datereceived);


CREATE FOREIGN TABLE ftw_cfpb(
	datereceived char(10),
	product varchar(30),
	subproduct varchar(30),
	issue varchar(50),
	subissue varchar(40),
	consumercomp varchar(30),
	comapnypresp varchar(50),
	company varchar(50),
	state varchar(9),
	zip varchar(5),
	tags varchar(20),
	consumer varchar(20),
	submitted varchar(10),
	datesent char(10),
	compres char(30),
	timerep char(3),
	consdisp char(3),
	compid  char(7) 
) SERVER obliv;

CREATE FUNCTION update_mapping() RETURNS void AS 
$$
	DECLARE
		ftw_users_oid oid;
		user_oid oid;
		user_email_oid oid;
	BEGIN
		ftw_users_oid := 0;
		user_oid := 0;
		user_email_oid := 0;
		select Oid from pg_class into ftw_users_oid  where relname  = 'ftw_cfpb';

		select Oid from pg_class into user_oid where relname = 'cfpb';

		select Oid from pg_class into user_email_oid  where relname  = 'cfpb_date';

		/*insert into obl_ftw (ftw_table_oid, mirror_table_oid, mirror_index_oid, ftw_table_nblocks, ftw_index_nblocks, init) values(ftw_users_oid, user_oid, user_email_oid, 2850, 500, false);*/
		insert into obl_ftw (ftw_table_oid, mirror_table_oid, mirror_index_oid, ftw_table_nblocks, ftw_index_nblocks, init) values(ftw_users_oid, user_oid, user_email_oid, 2850, 700, false);

	END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION get_ftw_oid() RETURNS oid AS
$$
	DECLARE
		ftw_users_oid oid;
	BEGIN
		select Oid from pg_class into ftw_users_oid  where relname  = 'ftw_cfpb';
		RETURN ftw_users_oid;
	END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION get_original_index_oid() RETURNS oid AS
$$
	DECLARE
		original_index_oid oid;
	BEGIN
		select Oid from pg_class into original_index_oid  where relname  = 'complaints_datereceived';
		RETURN original_index_oid;
	END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_original_heap_oid() RETURNS oid AS
$$
	DECLARE
		original_index_oid oid;
	BEGIN
		select Oid from pg_class into original_index_oid  where relname  = 'complaints';
		RETURN original_index_oid;
	END;
$$ LANGUAGE plpgsql;

select update_mapping();


-- Example of commands to run olbiv_fdw on the dynamic setting where tuples are inserted on the table with insert cmds.


-- This dynamic setting is assumed to use Papth ORAM as the ORAM lib and is the baseline for comparision for the
-- research paper. In this setting the OST protocol is not used.


--select open_enclave();


-- The first argument of init_soe (0) sets the  oblivpg_fdw mode to dynamic.
-- In this mode, the SOE does not use the OST protocol and expects table tuples to be inserted with SQL insert command.
--select init_soe(0, CAST( get_ftw_oid() as INTEGER), 1, CAST (get_original_index_oid() as INTEGER));


-- Postgres command to run external file. The inser_obl.sql file contains insert for the oblivious table obl_ftw.

--\i cfpb/inserts_obl.sql

-- Postgres timing command that meseasures query execution time.
--\timing



-- Execute example equality query on the oblivious table that uses the oblivious index.

--select datereceived from ftw_cfpb where datereceived = '06/11/2019';


-- Execute example greater Than Or Equal to query on the oblivious table that uses the oblivious index.

--select datereceived from ftw_cfpb where datereceived >= '06/11/2019';

-- Closes the Enclave and frees resources used.
--select close_enclave();


-- Example of commands to run olbiv_fdw on the FOREST setting where table and index records are loaded from 
-- a non-oblivious table to the corresponding oblivious tables.

-- This FOREST ORAM setting is assumed to use Forest  ORAM as the ORAM lib and is the optimized system for comparision for the
-- research paper. In this setting the OST protocol is used. However, to just measure the performance gains of OST, the
-- Path ORAM scheme can be used instead of Forest ORAM.


--select open_enclave();


-- The first argument of init_soe (1) sets the  oblivpg_fdw mode to FOREST.
-- In this mode, the SOE uses the  OST protocol and expects table tuples to be inserted with the oblivpg_fdw load_blocks command.
--select init_soe(1, CAST( get_ftw_oid() as INTEGER), 1, CAST (get_original_index_oid() as INTEGER));


-- Oblivpg_ftw command to load blocks from insecure tables to oblivious and encrypted tables.

--select load_blocks(CAST (get_original_index_oid() as INTEGER), CAST (get_original_heap_oid() as INTEGER));

-- Postgres timing command that meseasures query execution time.
--\timing


-- Execute example equality query on the oblivious table that uses the oblivious index.

--select datereceived from ftw_cfpb where datereceived = '06/11/2019';


-- Execute example greater Than Or Equal to query on the oblivious table that uses the oblivious index.

--select datereceived from ftw_cfpb where datereceived >= '06/11/2019';

-- Closes the Enclave and frees resources used.
--select close_enclave();

