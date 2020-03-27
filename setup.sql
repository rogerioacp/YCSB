GRANT ALL PRIVILEGES ON DATABASE test to rogerio;

-- Plaintext Table
CREATE TABLE usertable (
	YCSB_KEY char(10), 
	YCSB_VALUE char(8000)
);
alter table usertable alter column YCSB_VALUE set storage PLAIN;

create index usertable_key on usertable using btree (YCSB_KEY);
