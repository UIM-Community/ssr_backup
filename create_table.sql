CREATE TABLE ssrbackup_ref (
    profileId int not null,
	profileName varchar(255) not null,
	template smallint not null,
	group_id int,
	groupName varchar(100),
	device_id int,
	deviceName varchar(100),
	updated datetime,
	created datetime DEFAULT GETDATE(),
	ver_id int not null DEFAULT 1,
	cs_id int
)