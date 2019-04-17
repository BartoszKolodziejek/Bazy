use Forex;

create procedure fillSymbolsAndIntervals
as
begin 
insert into symbols (name, pips_value) values ('EURUSD', 0.0001),
											  ('JPYUSD', 0.01),
											  ('XAUUSD', 0.01),
											  ('NAS100', 1);


insert into intervals (name, duration) values ('M1', '00:01'), 
											 ('M5', '00:05'),
											 ('M15', '00:15'),
											 ('M30', '00:30'),
											 ('H1', '01:00'),
											 ('H4', '04:00'),
											 ('D1', '23:59:59');
	end
	go;

create procedure fillCandles
@symbol varchar(10),
@interval varchar(10),
@path varchar(MAX)
AS
begin
declare @symbol_id int;
declare @interval_id int;
select @symbol_id = id 
from symbols
where name like @symbol;
select @interval_id = id 
from intervals
where name like @interval;
create table #tempCandles (
                        Date varchar(50) NOT NULL,
                        Low varchar(10) NOT NULL,
						High varchar(10) NOT NULL,
						"Open" varchar(10) NOT NULL,
						"Close" varchar(10) NOT NULL,
						"Volume" integer NOT NULL
                       );
declare @sql VARCHAR(MAX);
set @sql =  'BULK INSERT #tempCandles
FROM ''' +@path + ''' 
WITH
(
  FIRSTROW = 1,
  DATAFILETYPE=''widechar'', -- UTF-16
  FIELDTERMINATOR = '','',
  ROWTERMINATOR = ''\n''
)'
exec (@sql);
insert into candles ("Date", High, Low, "Open", "Close", SYMBOLS_ID, INTERVALS_ID)
 select DATETIMEFROMPARTS 
 ( SUBSTRING(Date, 7, 4),
   SUBSTRING(Date, 4, 2),
   SUBSTRING(Date, 1, 2),
   SUBSTRING(Date, 12, 2),
   SUBSTRING(Date, 15, 2),
   '0', '0') "Date",
   CONVERT(decimal (20,5), Low) Low,
   CONVERT(decimal (20,5), High) High, 
   CONVERT(decimal (20,5), "Open") "Open",
   CONVERT(decimal (20,5), "Close") "Close",
   @symbol_id SYMBOLS_ID,
   @interval_id INTERVALS_ID
   from #tempCandles;
drop table #tempCandles;
end
go;
	
exec fillSymbolsAndIntervals
exec fillCandles 'EURUSD', 'M1', 'C:\Users\barto\Downloads\EURUSD_Candlestick_1_m_BID_30.03.2019-30.03.2019.csv' 
	
USE [Projekt Bazy Danych]
GO
/****** Object:  StoredProcedure [dbo].[fillUsers]    Script Date: 17.04.2019 11:24:35 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER procedure [dbo].[fillUsers]
as
begin
insert into users (id, name, last_name, nationality, email, mobile_number) values (1, 'John', 'Murphy', 'UK', 'john.murphy@gmail.com', 708936172),
										  (2, 'Tom', 'Smith', 'UK', 'tom.smith@gmail.com', 564729462),
										  (3, 'Erinn', 'McBride', 'US', 'er.mcbrude@gmail.com', 123321123),
									          (4,'Jan', 'Kowal', 'PL', 'janek.kow@gmail.com', 768764532),
										  (5, 'Robert', 'Walus', 'PL', 'robi321@gmail.com', 672087213),
										  (6, 'Agnieszka', 'Walek', 'PL', 'aga.wal@gmail.com', 305927591),
									          (7, 'Sylwester', 'Brown', 'US', 'sylbrown@gmail.com', 987345321);
end
go;
																			  end;

