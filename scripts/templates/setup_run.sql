CREATE FUNCTION update_mapping_run() RETURNS void AS 
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
        
        delete from obl_ftw where ftw_table_oid = ftw_users_oid;

		insert into obl_ftw (ftw_table_oid, mirror_table_oid, mirror_index_oid, ftw_table_nblocks, ftw_index_nblocks, init) values(ftw_users_oid, user_oid, user_email_oid, {T_NBLOCKS}, {I_NBLOCKS}, false);

	END;
$$ LANGUAGE plpgsql;

select update_mapping_run();
