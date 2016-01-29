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
	is_complete boolean,
	primary key ((dt,channel), path)
);

create table channel (
	channel int primary key,
	check_date timestamp
);

insert into content(path,last_segment,revision,body,created_at,modified_at)
values  ('plain-resource','',1,'This is plain resource',dateof(now()),null);

insert into content(path,last_segment,revision,body,created_at,modified_at)
values  ('plain-resource-incomplete','',1,'This is plain resource (incomplete)',dateof(now()),null);
