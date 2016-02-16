create table content (
	document_uri text,
	item_segment text,
	revision bigint static,
	monitor_dt_quantum timestamp static,
	monitor_uuid timeuuid static,
	monitor_channel int static,
	body text,
	is_deleted boolean,
	created_at timestamp,
	modified_at timestamp,
	primary key (document_uri, item_segment)
);

create table monitor (
	dt_quantum timestamp,
	channel int,
	uri text,
	revision bigint,
	uuid timeuuid,
	body text,
	completed_at timestamp,
	primary key ((dt_quantum, channel), uri, revision, uuid)
);

create table channel (
	channel int primary key,
	last_quantum timestamp
);

