drop table if exists indexing_urls;
create table indexing_urls
(
	id INTEGER not null
		constraint indexing_urls_pk
			primary key autoincrement,
	url str,
	status str,
	created datetime,
	updated datetime
);

create unique index indexing_urls_id_uindex
	on indexing_urls (id);

create unique index indexing_urls_url_uindex
	on indexing_urls (url);
