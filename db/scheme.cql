create table content (
	path text,
	last_segment text,
	revision bigint static,
	monitor_dt timestamp static,
	monitor_channel int static,
	body text,
	is_deleted boolean,
	created_at timestamp,
	modified_at timestamp,
	primary key (path, last_segment)
);

create table monitor (
	dt timestamp,
	channel int,
	path text,
	revision bigint,
	body text,
	completed_at timestamp,
	primary key ((dt, channel), path, revision)
);

create table channel (
	channel int primary key,
	check_date timestamp
);

