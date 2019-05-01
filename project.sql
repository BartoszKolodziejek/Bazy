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
           "volume" VARCHAR(MAX) NOT NULL 
        ); 

      DECLARE @sql VARCHAR(max); 

      SET @sql = 'BULK INSERT dbo.[#tempCandles] FROM ''' + @path+'''  WITH ( firstrow = 2,   DATAFILETYPE=''widechar'',  CODEPAGE=65001, FIELDTERMINATOR = '','',   ROWTERMINATOR = ''\n'' );'

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
declare @symbol_id integer
select @symbol_id = id 
from symbols
where name like @symbol;
declare @lastCandleClose decimal (20, 5);
SELECT  @lastCandleClose = "Close"
     FROM  candles 
         JOIN (SELECT Max(id) 
         last_id 
         FROM   candles 
		 where candles.INTERVALS_ID = 8
         GROUP  BY symbols_id) 
         maxIds 
         ON maxIds.last_id = candles.id
		 join symbols on symbols.id = candles.SYMBOLS_ID
		 where symbols.name = @Symbol
if  (@type = 'SELL' or @type = 'BUY')
insert into orders(type, Date, Stop_Loss, Take_Profit, price, ACCOUNTS_ID, SYMBOLS_ID) 
values (@type, @Date, @StopLoss, @TakeProfit, @lastCandleClose, @AccountId, @symbol_id)
else 
begin
if (@type = 'BUYSTOP' or  @type = 'SELLLIMIT') and @price is not null and @price <  @lastCandleClose 
raiserror('couldn`t make order', 20, -1) with log;
if (@type = 'BUYLIMIT' or @type = 'SELLSTOP') and @price is not null and @price >  @lastCandleClose 
raiserror('couldn`t make order', 20, -1) with log;
insert into orders (type, Date, Stop_Loss, Take_Profit, price, ACCOUNTS_ID, SYMBOLS_ID) 
values (@type, @Date, @StopLoss, @TakeProfit, @price, @AccountId, @symbol_id)
end
end;

ALTER PROCEDURE UpdatePositions 
AS 
  BEGIN 
      BEGIN try 
          BEGIN TRANSACTION 
		            DELETE FROM positions 
          WHERE  positions.id = ANY (SELECT positions.id 
                                     FROM   positions 
                                            JOIN orders 
                                              ON orders.id = positions.orders_id 
                                            JOIN (SELECT "close", 
                                                         symbols_id 
                                                  FROM   candles 
                                                         JOIN (SELECT Max(id) 
                                                              last_id 
                                                               FROM   candles 
															   where candles.INTERVALS_ID = 8
                                                 GROUP  BY symbols_id) 
                                                              maxIds 
                                                           ON maxIds.last_id = 
                                                              candles.id) 
                                                 lastClose 
                                              ON lastClose.symbols_id = 
                                                 orders.symbols_id 
                                            JOIN symbols 
                                              ON symbols.id = orders.symbols_id 
                                     WHERE  type LIKE '%BUY%' 
                                            AND "stop_loss" > "close") 

          DELETE FROM positions 
          WHERE  positions.id = ANY (SELECT positions.id 
                                     FROM   positions 
                                            JOIN orders 
                                              ON orders.id = positions.orders_id 
                                            JOIN (SELECT "close", 
                                                         symbols_id 
                                                  FROM   candles 
                                                         JOIN (SELECT Max(id) 
                                                              last_id 
                                                               FROM   candles 
															   where candles.INTERVALS_ID = 8
                                                 GROUP  BY symbols_id) 
                                                              maxIds 
                                                           ON maxIds.last_id = 
                                                              candles.id) 
                                                 lastClose 
                                              ON lastClose.symbols_id = 
                                                 orders.symbols_id 
                                            JOIN symbols 
                                              ON symbols.id = orders.symbols_id 
                                     WHERE  type LIKE '%SELL%' 
                                            AND "stop_loss" < "close") 


          UPDATE positions 
          SET    positions."status" = currentStatusSelect.currentstatus 
          FROM   (SELECT positions.id, 
                         ( "close" - price ) * 1 / symbols.pips_value 
                         currentStatus 
                  FROM   positions 
                         JOIN orders 
                           ON orders.id = positions.orders_id 
                         JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candles 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candles 
											where candles.INTERVALS_ID = 8
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candles.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
                         JOIN symbols 
                           ON symbols.id = orders.symbols_id 
                  WHERE  type LIKE '%BUY%') currentStatusSelect 
                 JOIN positions 
                   ON positions.id = currentStatusSelect.id 

          UPDATE positions 
          SET    positions."status" = currentStatusSelect.currentstatus 
          FROM   (SELECT positions.id, 
                         ( price - "close" ) * 1 / symbols.pips_value 
                         currentStatus 
                  FROM   positions 
                         JOIN orders 
                           ON orders.id = positions.orders_id 
                         JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candles 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candles 
											where candles.INTERVALS_ID = 8
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candles.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
                         JOIN symbols 
                           ON symbols.id = orders.symbols_id 
                  WHERE  type LIKE '%SELL%') currentStatusSelect 
                 JOIN positions 
                   ON positions.id = currentStatusSelect.id 

          COMMIT TRANSACTION 
      END try 

      BEGIN catch 
          ROLLBACK TRANSACTION; 

          PRINT 'could not update positions'; 

          THROW; 
      END catch; 
  END; 

ALTER procedure execStopAndLimits
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

				insert into Forex.dbo.positions (id, "Date", "Status", "ORDERS_ID")
							select NEXT VALUE FOR postion_sequence, @now, (price- lastClose."Close")* 1 / symbols.pips_value, orders.id
					from orders 
					left join positions on positions.ORDERS_ID = orders.id
					JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candles 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candles 
											where candles.INTERVALS_ID = 8
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candles.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
					JOIN symbols on symbols.id = orders.SYMBOLS_ID
					where type like 'SELLSTOP'
					 and positions.id is null
					 and orders.price < lastClose."Close"


				insert into positions (id, "Date", "Status", "ORDERS_ID")
					select NEXT VALUE FOR postion_sequence, @now, (price- lastClose."Close")* 1 / symbols.pips_value, orders.id
					from orders 
					left join positions on positions.ORDERS_ID = orders.id
					JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candles 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candles 
											where candles.INTERVALS_ID = 8
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candles.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
					JOIN symbols on symbols.id = orders.SYMBOLS_ID
					where type like 'SELLLIMIT'
					 and positions.id is null
					 and orders.price > lastClose."Close"

			insert into positions (id, "Date", "Status", "ORDERS_ID")
				select NEXT VALUE FOR postion_sequence, @now, (lastClose."Close" - price)* 1 / symbols.pips_value, orders.id
					from orders 
					left join positions on positions.ORDERS_ID = orders.id
					JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candles 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candles 
											where candles.INTERVALS_ID = 8
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candles.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
					JOIN symbols on symbols.id = orders.SYMBOLS_ID
					where type like 'BUYSTOP'
					 and positions.id is null
					 and orders.price > lastClose."Close"

					 insert into positions (id, "Date", "Status", "ORDERS_ID")
				select NEXT VALUE FOR postion_sequence, @now, (lastClose."Close" - price)* 1 / symbols.pips_value, orders.id
					from orders 
					left join positions on positions.ORDERS_ID = orders.id
					JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candles 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candles 
											where candles.INTERVALS_ID = 8
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candles.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
					JOIN symbols on symbols.id = orders.SYMBOLS_ID
					where type like 'BUYLIMIT'
					 and positions.id is null
					 and orders.price < lastClose."Close"

				commit transaction
			end try
			begin catch
			rollback transaction;
			print 'could not open orders';
			throw;
			end catch
		end;	
end;

alter procedure execMarketExecution 
 as 
	if exists (select * from orders where type in ('BUY', 'SELL'))
		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION

					declare @now datetime;
					set @now = GETDATE();

				insert into Forex.dbo.positions (id, "Date", "Status", "ORDERS_ID")
							select NEXT VALUE FOR postion_sequence, @now, 0, orders.id
					from orders 
					left join positions on positions.ORDERS_ID = orders.id
					JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candles 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candles 
											where candles.INTERVALS_ID = 8
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candles.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
					JOIN symbols on symbols.id = orders.SYMBOLS_ID
					where type = 'SELL'
					 and positions.id is null

			insert into positions (id, "Date", "Status", "ORDERS_ID")
				select NEXT VALUE FOR postion_sequence, @now, 0, orders.id
					from orders 
					left join positions on positions.ORDERS_ID = orders.id
					JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candles 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candles 
											where candles.INTERVALS_ID = 8
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candles.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
					JOIN symbols on symbols.id = orders.SYMBOLS_ID
					where type = 'BUY'
					 and positions.id is null

					 COMMIT TRANSACTION
				END TRY
				BEGIN CATCH
					rollback transaction;
					print 'could not open orders';
					throw;
				END CATCH
			END
			

create function getAccountPositions (@accountsId Int)
returns table
as
return
(
select positions.id,
			 positions.Date,
			 positions.Status, 
			 orders.Stop_Loss, 
			 orders.Take_Profit, 
			 symbols.name from accounts
			join orders on orders.ACCOUNTS_ID = accounts.id
			join positions on positions.ORDERS_ID = orders.id
			join symbols on symbols.id = orders.SYMBOLS_ID
			where ACCOUNTS_ID = @accountsId)

create function getAccountBalance(@accountsId Int)
returns decimal(38, 2)
as
begin
declare @balance decimal(38, 2);
select @balance = accounts.balance from accounts
where id = @accountsId
return @balance
end



alter function getNumberOrdersOfType(@accountsId Int, @type varchar(10))
returns @allOrdersTable table
( number int,
  Symbol varchar(10)
)
as
begin 
	if @type in (select "type" from orders where ACCOUNTS_ID = @accountsId )
	insert into @allOrdersTable 
	select count(orders.id) number, symbols.name from orders
	join symbols on symbols.id = Symbols_Id
	where orders.ACCOUNTS_ID = @accountsId 
	and orders.type like @type
	group by symbols.name
	else
	insert into @allOrdersTable values (0, 'NONE')
return
end

  create TRIGGER UpdatePositionsTrigger
  ON candles
  AFTER INSERT
  AS
  begin
  exec execStopAndLimits
  exec UpdatePositions
  end
  go

  alter TRIGGER MarketExecutionTrigger
  ON [dbo].orders
  FOR INSERT, UPDATE 
  AS
  begin
  exec execMarketExecution
  end


  use Forex

delete  from positions
exec fillSymbolsAndIntervals
exec fillCandles 'EURUSD', 'M1', 'C:\Users\barto\Downloads\dane\EURUSD_Candlestick_1_m_BID_29.04.2016-27.04.2019.csv'
exec fillCandles 'JPYUSD', 'M1', 'C:\Users\barto\Downloads\dane\USDJPY_Candlestick_1_m_BID_29.04.2016-27.04.2019.csv'
exec fillCandles 'XAUUSD', 'M1', 'C:\Users\barto\Downloads\dane\XAUUSD_Candlestick_1_m_BID_27.04.2016-27.04.2019.csv'
exec fillCandles 'NAS100', 'M1', 'C:\Users\barto\Downloads\dane\USA500.IDXUSD_Candlestick_1_m_BID_27.04.2016-27.04.2019.csv'
exec [fillUsers]
exec [fillAccounts]
DECLARE @tmp DATETIME
SET @tmp = GETDATE()
exec makeOrder 'SELLSTOP', @tmp, 5.0000, 1.0000, 1, 'EURUSD', 1.00
SET IDENTITY_INSERT positions ON
SET IDENTITY_INSERT orders OFF
exec execStopAndLimits
exec UpdatePositions
use Forex

CREATE TABLE candlesNoIndexes
    (
    id        INTEGER NOT NULL IDENTITY(1,1),
    low       decimal (20, 5),
    high      decimal (20, 5),
    "Open"    decimal (20, 5),
    "Close"   decimal (20, 5),
     Date DATETIME , 
     SYMBOLS_ID INTEGER NOT NULL , 
     INTERVALS_ID INTEGER NOT NULL );

	 insert into candlesNoIndexes (low, high, "Open", "Close", "Date", "SYMBOLS_ID", "INTERVALS_ID") select low, high, "Open", "Close", "Date", "SYMBOLS_ID", "INTERVALS_ID" from  candles

	 select * from candles
---clustered indexes
DECLARE @now DATETIME
SET @now = GETDATE()
SET STATISTICS IO, TIME ON 
select NEXT VALUE FOR postion_sequence, @now, (lastClose."Close" - price)* 1 / symbols.pips_value, orders.id
					from orders 
					left join positions on positions.ORDERS_ID = orders.id
					JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candles 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candles 
											where candles.INTERVALS_ID = 8
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candles.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
					JOIN symbols on symbols.id = orders.SYMBOLS_ID

select NEXT VALUE FOR postion_sequence, @now, (lastClose."Close" - price)* 1 / symbols.pips_value, orders.id
					from orders 
					left join positions on positions.ORDERS_ID = orders.id
					JOIN (SELECT "close", 
                                      symbols_id 
                               FROM   candlesNoIndexes 
                                      JOIN (SELECT Max(id) last_id 
                                            FROM   candlesNoIndexes
											where candlesNoIndexes.INTERVALS_ID = 8 
                                            GROUP  BY symbols_id) maxIds 
                                        ON maxIds.last_id = candlesNoIndexes.id) 
                              lastClose 
                           ON lastClose.symbols_id = orders.symbols_id 
					JOIN symbols on symbols.id = orders.SYMBOLS_ID
SET STATISTICS IO, TIME ON 
select max("Close") from candles
select max("Close") from candlesNoIndexes
CREATE NONCLUSTERED INDEX NON_CLUSTERED_CLOSE ON candles("Close")

