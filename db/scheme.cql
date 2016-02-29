create table content (
	document_uri text,
	item_segment text,
	revision bigint static,
	transaction_list list<timeuuid> static,
	body text,
	is_deleted boolean,
	created_at timestamp,
	modified_at timestamp,
	primary key (document_uri, item_segment)
);

create table transaction (
	dt_quantum bigint,
	partition int,
	uri text,
	uuid timeuuid,
	revision bigint,
	body text,
	completed_at timestamp,
	primary key ((dt_quantum, partition), uri, uuid)
);

create table checkpoint (
	partition int primary key,
	last_quantum bigint
);

