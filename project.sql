use Forex;

create procedure fillSymbolsAndIntervals
as
begin 
begin try
begin transaction 
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
commit transaction;
end try
begin catch
print 'couldn`t fill symbols and intervals'
rollback transaction;
throw;
end catch


	end
	go;

Alter PROCEDURE fillCandles @symbol   VARCHAR(10), 
                             @interval VARCHAR(10), 
                             @path     VARCHAR(max) 
AS 
  BEGIN 
	  BEGIN TRY
      BEGIN TRANSACTION 
      DECLARE @symbol_id INT; 
      DECLARE @interval_id INT; 

      SELECT @symbol_id = id 
      FROM   symbols 
      WHERE  NAME LIKE @symbol; 

      SELECT @interval_id = id 
      FROM   intervals 
      WHERE  NAME LIKE @interval; 

      CREATE TABLE #tempcandles 
        ( 
           date     VARCHAR(50) NOT NULL, 
           low      VARCHAR(10) NOT NULL, 
           high     VARCHAR(10) NOT NULL, 
           "open"   VARCHAR(10) NOT NULL, 
           "close"  VARCHAR(10) NOT NULL, 
           "volume" INTEGER NOT NULL 
        ); 

      DECLARE @sql VARCHAR(max); 

      SET @sql = 'BULK INSERT dbo.[#tempCandles] FROM ''' + @path+'''  WITH ( firstrow = 1,   DATAFILETYPE=''widechar'',  CODEPAGE=65001, FIELDTERMINATOR = '','',   ROWTERMINATOR = ''\n'' );'

      EXEC (@sql); 

      INSERT INTO candles 
                  ("date", 
                   high, 
                   low, 
                   "open", 
                   "close", 
                   symbols_id, 
                   intervals_id) 
      SELECT Datetimefromparts (Substring(date, 7, 4), Substring(date, 4, 2), 
                    Substring(date, 1, 2), Substring(date, 12, 2), 
             Substring(date, 15, 2), 
                    '0', '0')                  "Date", 
             CONVERT(DECIMAL (20, 5), low)     Low, 
             CONVERT(DECIMAL (20, 5), high)    High, 
             CONVERT(DECIMAL (20, 5), "open")  "Open", 
             CONVERT(DECIMAL (20, 5), "close") "Close", 
             @symbol_id                        [SYMBOLS_ID], 
             @interval_id                      [INTERVALS_ID] 
      FROM   #tempcandles; 

      DROP TABLE #tempcandles; 

      COMMIT TRANSACTION;
	  END TRY
	  BEGIN CATCH
	  rollback transaction;
	  print 'could not process the file';
		throw;
	  END CATCH;
  END;
go 

	

	
USE [Forex]
GO
/****** Object:  StoredProcedure [dbo].[fillUsers]    Script Date: 17.04.2019 11:24:35 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [dbo].[fillUsers]
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

	
USE [Forex]
GO
/****** Object:  StoredProcedure [dbo].[fillAccounts]    Script Date: 17.04.2019 11:35:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [dbo].[fillAccounts]
as
begin
insert into accounts (balance, currency, margin_level, users_id) values (10000,'GBP',1500,1),
									(1500,'GBP',1500,1),
									(2053,'GBP',200,2),
									(21344,'GBP',200,2),
									(12231,'USD',140,3),
									(1234,'USD',140,3),
									(4432,'PLN',350,4),
									(665789,'PLN',350,4),
									(12355,'PLN',432,5),
									(6654,'PLN',432,5),
									(9887,'PLN',223,6),
									(90009,'PLN',223,6),
									(34908,'USD',105,7),
									(1251,'USD',105,7);
end
go;

select * from orders

ALTER procedure makeOrder
@type varchar(10),
@Date datetime,
@StopLoss decimal,
@TakeProfit decimal, 
@AccountId integer,
@Symbol varchar(10),
@price decimal(20, 5)
as
begin
begin try
declare @symbol_id integer
select @symbol_id = id 
from symbols
where name like @symbol;
declare @lastCandleClose decimal (20, 5);
					select @lastCandleClose = "close" from ( 
					select MAX(id) last_id from candles) max_id
					join candles on max_id.last_id = candles.id;
if (@type = 'BUYSTOP' or  @type = 'SELLLIMIT') and @price <  @lastCandleClose 
raiserror('couldn`t make order', 20, -1) with log;
if (@type = 'BUYLIMIT' or @type = 'SELLSTOP') and @price >  @lastCandleClose 
raiserror('couldn`t make order', 20, -1) with log;
insert into orders values (@type, @Date, @StopLoss, @TakeProfit, @price, @AccountId, @symbol_id)
end try
begin catch
print 'couldn`t make order'
end catch
end
go;

create procedure execStopAndLimits
as
begin
	if exists (select * from orders where type in ('BUYSTOP', 'SELLSTOP', 'BUYLIMIT', 'SELLLIMIT'))
	and exists (select * from orders 
					left join positions on positions.id = orders.id
					 where type in ('BUYSTOP', 'SELLSTOP', 'BUYLIMIT', 'SELLLIMIT')
					 and positions.id is null)
		begin
			begin try
				begin transaction
					declare @now datetime;
					set @now = GETDATE();
					declare @lastCandleClose decimal (20, 5);
					select @lastCandleClose = "close" from ( 
					select MAX(id) last_id from candles) max_id
					join candles on max_id.last_id = candles.id;
					insert into Forex.dbo.positions (id, "Date", "Status", "ORDERS_ID")
					select NEXT VALUE FOR postion_sequence, @now, price - @lastCandleClose, orders.id
					from orders 
					left join positions on positions.id = orders.id
					where type like 'SELLLIMIT'
					 and positions.id is null
					 and orders.price > @lastCandleClose;
					 insert into positions (id, "Date", "Status", "ORDERS_ID")
					select NEXT VALUE FOR postion_sequence, @now, price - @lastCandleClose, orders.id
					from orders 
					left join positions on positions.id = orders.id
					where type like 'SELLSTOP'
					 and positions.id is null
					 and orders.price < @lastCandleClose;
					 insert into positions (id, "Date", "Status", "ORDERS_ID")
					select NEXT VALUE FOR postion_sequence, @now, @lastCandleClose - price, orders.id
					from orders 
					left join positions on positions.id = orders.id
					where type like 'BUYLIMIT'
					 and positions.id is null
					 and orders.price < @lastCandleClose;
					insert into positions (id, "Date", "Status", "ORDERS_ID")
					select NEXT VALUE FOR postion_sequence, @now, @lastCandleClose - price, orders.id
					from orders 
					left join positions on positions.id = orders.id
					where type like 'BUYSTOP'
					 and positions.id is null
					 and orders.price > @lastCandleClose;
				commit transaction
			end try
			begin catch
			rollback transaction;
			print 'could not open orders';
			throw;
			end catch
		end;	
end;


exec fillSymbolsAndIntervals
exec fillCandles 'EURUSD', 'M1', 'C:\Users\barto\Downloads\EURUSD_Candlestick_1_m_BID_30.03.2019-30.03.2019.csv'
exec [fillUsers]
exec [fillAccounts]
DECLARE @tmp DATETIME
SET @tmp = GETDATE()
exec makeOrder 'BUYSTOP', @tmp, 1.0000, 5.0000, 1, 'EURUSD', 1.3789
SET IDENTITY_INSERT positions ON
exec execStopAndLimits


