drop table peter;
GO
drop table peterBinary;
GO

create table peter(
id timestamp,
	C0 float,
	C1 float,
	C2 float,
	C3 float,
	C4 float,
	C5 float,
	C6 float,
	C7 float,
	C8 float,
	C9 float,
	C10 float,
	C11 float,
	C12 float,
	C13 float,
	C14 float,
	C15 float,
	C16 float,
	C17 float,
	C18 float,
	C19 float,
	C20 float,
	C21 float,
	C22 float,
	C23 float,
	C24 float,
	C25 float,
	C26 float,
	C27 float,
	C28 float,
	C29 float,
	C30 float,
	C31 float,
	C32 float,
	C33 float,
	C34 float,
	C35 float,
	C36 float,
	C37 float,
	C38 float,
	C39 float,
	C40 float,
	C41 float,
	C42 float,
	C43 float,
	C44 float,
	C45 float,
	C46 float,
	C47 float,
	C48 float,
	C49 float
);

create table peterBinary(ts timestamp, bin varbinary(2000));

select count(*) from peter -- 15,172 MB

select count(*) from peterBinary --- 766,586 MB

select top 100 * from peterBinary 