create table content (
	document_uri text,
	item_segment text,
	revision bigint static,
	monitor_dt timestamp static,
	monitor_channel int static,
	body text,
	is_deleted boolean,
	created_at timestamp,
	modified_at timestamp,
	primary key (document_uri, item_segment)
);

create table monitor (
	dt timestamp,
	channel int,
	uri text,
	revision bigint,
	body text,
	completed_at timestamp,
	primary key ((dt, channel), uri, revision)
);

create table channel (
	channel int primary key,
	check_date timestamp
);

