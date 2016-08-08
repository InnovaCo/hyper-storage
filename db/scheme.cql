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
	document_uri text,
	uuid timeuuid,
	revision bigint,
	body text,
	completed_at timestamp,
	primary key ((dt_quantum, partition), document_uri, uuid)
);

create table checkpoint (
	partition int primary key,
	last_quantum bigint
);

create table index_meta(
    document_uri text,
    index_id text,
    status int,
    sort_by text,
    filter_by text,
    table_name text,
    meta_transaction_id timeuuid,
    primary key (document_uri, index_id)
);

create table pending_indexes(
    partition int,
    document_uri text,
    index_id text,
    last_item_segment text,
    meta_transaction_id timeuuid,
    primary key (partition, document_uri, index_id, meta_transaction_id)
);

create table index_content(
    document_uri text,
    item_segment text,
    revision bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key (document_uri, item_segment)
);

create table index_content_ta0(
    document_uri text,
    t0 text,
    item_segment text,
    revision bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key (document_uri, t0, item_segment)
);

create table index_content_da0(
    document_uri text,
    d0 decimal,
    item_segment text,
    revision bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key (document_uri, d0, item_segment)
);

create table index_content_td0(
    document_uri text,
    t0 text,
    item_segment text,
    revision bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key (document_uri, t0, item_segment)
) with clustering order by (t0 desc);

create table index_content_dd0(
    document_uri text,
    d0 decimal,
    item_segment text,
    revision bigint static,
    body text,
    created_at timestamp,
    modified_at timestamp,
    primary key (document_uri, d0, item_segment)
) with clustering order by (d0 desc);

