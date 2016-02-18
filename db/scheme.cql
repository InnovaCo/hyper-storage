create table content (
	document_uri text,
	item_segment text,
	revision bigint static,
	monitor_list list<timeuuid> static,
	body text,
	is_deleted boolean,
	created_at timestamp,
	modified_at timestamp,
	primary key (document_uri, item_segment)
);

create table monitor (
	dt_quantum bigint,
	channel int,
	uri text,
	uuid timeuuid,
	revision bigint,
	body text,
	completed_at timestamp,
	primary key ((dt_quantum, channel), uri, uuid)
);

create table channel (
	channel int primary key,
	last_quantum bigint
);

