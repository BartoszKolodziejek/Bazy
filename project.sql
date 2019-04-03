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