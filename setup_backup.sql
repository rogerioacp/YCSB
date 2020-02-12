GRANT ALL PRIVILEGES ON DATABASE test to rogerio;

CREATE EXTENSION oblivpg_fdw;


-- Plaintext Table
CREATE TABLE usertable (
	YCSB_KEY char(10), 
	YCSB_VALUE char(1300)
);


create index usertable_key on usertable using btree (YCSB_KEY);


-- Mirror Table that the Foreign Data Wrapper Writes/Read from

CREATE TABLE mirror_usertable (
	YCSB_KEY char(10), 
	YCSB_VALUE char(1300)
);


create index mirror_usertable_key on mirror_usertable using btree (YCSB_KEY);

-- foreign table interface used by the client
CREATE FOREIGN TABLE ftw_usertable(
	YCSB_KEY char(10), 
	YCSB_VALUE char(1300)
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
		select Oid from pg_class into ftw_users_oid  where relname  = 'ftw_usertable';

		select Oid from pg_class into user_oid where relname = 'mirror_usertable';

		select Oid from pg_class into user_email_oid  where relname  = 'mirror_usertable_key';

		insert into obl_ftw (ftw_table_oid, mirror_table_oid, mirror_index_oid, ftw_table_nblocks, ftw_index_nblocks, init) values(ftw_users_oid, user_oid, user_email_oid, 48000, 6000, false);

	END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION get_ftw_oid() RETURNS oid AS
$$
	DECLARE
		ftw_users_oid oid;
	BEGIN
		select Oid from pg_class into ftw_users_oid  where relname  = 'ftw_usertable';
		RETURN ftw_users_oid;
	END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION get_original_index_oid() RETURNS oid AS
$$
	DECLARE
		original_index_oid oid;
	BEGIN
		select Oid from pg_class into original_index_oid  where relname  = 'usertable_key';
		RETURN original_index_oid;
	END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_original_heap_oid() RETURNS oid AS
$$
	DECLARE
		original_index_oid oid;
	BEGIN
		select Oid from pg_class into original_index_oid  where relname  = 'usertable';
		RETURN original_index_oid;
	END;
$$ LANGUAGE plpgsql;

select update_mapping();
