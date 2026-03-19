-- Creating DataWareHouse
create database datawarehouse;

select * from DatawarehouseSample

-- creating table dimCustomer
create table dimCustomer
(
    CustomerKey int identity primary key,
    CustomerId varchar(50),
    CustomerFirstName varchar(50),
    CustomerLastName varchar(50)
)

select *
from dimCustomer

-- Creating procedure spUpdateDimCustomer
create procedure spUpdateDimCustomer
as
begin
    -- insert
    insert into dimCustomer
    (
        CustomerId,
        CustomerFirstName,
        CustomerLastName
    )
    select distinct
        dw.CustomerId,
        dw.CustomerFirstName,
        dw.CustomerLastName
    from DatawarehouseSample dw
        left join dimCustomer dc
            on dw.CustomerId = dc.CustomerId
    where dc.CustomerId is null
    -- update
    update dc
    set dc.CustomerFirstName = dw.CustomerFirstName,
        dc.CustomerLastName = dw.CustomerLastName
    from dimCustomer dc
        inner join DatawarehouseSample dw
            on dc.CustomerId = dw.CustomerId
    where dc.CustomerFirstName <> dw.CustomerFirstName
          or dc.CustomerLastName <> dw.CustomerLastName
end

exec spUpdateDimCustomer

-- creating table dimEmployee
create table dimEmployee
(
    EmployeeKey int identity primary key,
    EmployeeId varchar(50),
    EmployeeFirstName varchar(50),
    EmployeeLastName varchar(50)
);

-- creating procedure spUpdateDimEmployee
create procedure spUpdateDimEmployee
as
begin
    -- insert
    insert into dimEmployee
    (
        EmployeeId,
        EmployeeFirstName,
        EmployeeLastName
    )
    select distinct
        dw.EmployeeId,
        dw.EmployeeFirstName,
        dw.EmpoyeeLastName as EmployeeLastName
    from DatawarehouseSample dw
        left join dimEmployee de
            on dw.EmployeeId = de.EmployeeId
    where de.EmployeeId is null
    -- update
    update de
    set de.EmployeeFirstName = dw.EmployeeFirstName,
        de.EmployeeLastName = dw.EmpoyeeLastName
    from dimEmployee de
        inner join DatawarehouseSample dw
            on de.EmployeeId = dw.EmployeeId
    where 
    de.EmployeeFirstName <> dw.EmployeeFirstName or
        de.EmployeeLastName <> dw.EmpoyeeLastName
end

exec spUpdateDimEmployee

-- creating table dimProducts
create table dimProducts
(
    ProductKey int identity primary key,
    ProductId varchar(50),
    ProductName varchar(50)
);

-- create procedure dimProducts
create procedure spUpdateDimProducts
as
begin
    -- insert
    insert into dimProducts
    (
        ProductId,
        ProductName
    )
    select distinct
        dw.ProductId,
        dw.ProudctName as ProductName
    from DatawarehouseSample dw
        left join dimProducts dp
            on dw.ProductId = dp.ProductId
    where dp.ProductId is null
    -- update
    update dp
    set dp.ProductName = dw.ProudctName
    from dimProducts dp
        inner join DatawarehouseSample dw
            on dw.ProductId = dp.ProductId
    where dp.ProductName <> dw.ProudctName
end

exec spUpdateDimProducts

-- create table dimDate
create table dimDate(
    Date date,
    DateKey int primary key,
    Day int,
    Month int,
    Year int,
    Quarter int
)

-- creating procedure spLoadDimDate

create procedure spLoadDimDate
as
begin

    with dateRange
    as (select min(OrderDate) as sDate,
               max(OrderDate) as maxDate
        from DatawarehouseSample
       ),
         allDates
    as (select sDate
        from dateRange
        union all
        select DATEADD(DAY, 1, sDate)
        from allDates
        where sDate <
        (
            select maxDate from dateRange
        )
       )
    insert into dimDate
    (
        Date,
        DateKey,
        Day,
        Month,
        Year,
        Quarter
    )
    select sDate,
           CAST(FORMAT(sDate, 'yyyyMMdd') AS int) AS DateKey,
           day(sDate) as Day,
           month(sDate) as Month,
           year(sDate) as Year,
           DATEPART(QUARTER, sDate) as Quarter
    from allDates
        left join dimDate dd
            on allDates.sDate = dd.Date
    where dd.Date is null
    OPTION (maxrecursion 0);

end

exec spLoadDimDate

-- creating factSales Table

create table factSales
(
    CustomerKey int,
    EmployeeKey int,
    ProductKey int,
    DateKey int,
    ProductPrice float,
    Quantity int,
    OrderDate date
        foreign key (CustomerKey) references dimCustomer (CustomerKey),
    foreign key (EmployeeKey) references dimEmployee (EmployeeKey),
    foreign key (ProductKey) references dimProducts (ProductKey),
    foreign key (DateKey) references dimDate (DateKey)
)

select *
from factSales

-- creating procedure spUpdateFactSales
create procedure spUpdateFactSales
as
begin
    -- inserting into factSales
    insert into factSales
    (
        CustomerKey,
        EmployeeKey,
        ProductKey,
        DateKey,
        ProductPrice,
        Quantity,
        OrderDate
    )
    select dc.CustomerKey,
           de.EmployeeKey,
           dp.ProductKey,
           dd.DateKey,
           dw.ProductPrice,
           dw.Quantity,
           dw.OrderDate
    from DatawarehouseSample dw
        inner join dimCustomer dc
            on dc.CustomerId = dw.CustomerId
        inner join dimEmployee de
            on de.EmployeeId = dw.EmployeeId
        inner join dimProducts dp
            on dp.ProductId = dw.ProductId
        inner join dimDate dd
            on dd.Date = dw.OrderDate
        left join factSales fs
            on fs.CustomerKey = dc.CustomerKey
               and fs.EmployeeKey = de.EmployeeKey
               and fs.ProductKey = dp.ProductKey
               and fs.DateKey = dd.DateKey
    where fs.CustomerKey is null

    -- update factSales
    update fs
    set fs.ProductPrice = dw.ProductPrice,
        fs.Quantity = dw.Quantity
    from factSales fs
        inner join DatawarehouseSample dw
            on fs.OrderDate = dw.OrderDate
        inner join dimCustomer dc
            on dw.CustomerId = dc.CustomerId
               and dc.CustomerKey = fs.CustomerKey
        inner join dimEmployee de
            on dw.EmployeeId = de.EmployeeId
               and de.EmployeeKey = fs.EmployeeKey
        inner join dimProducts dp
            on dw.ProductId = dp.ProductId
               and dp.ProductKey = fs.ProductKey
        inner join dimDate dd
            on dw.OrderDate = dd.Date
               and dd.DateKey = fs.DateKey
    where fs.ProductPrice <> dw.ProductPrice
          or fs.Quantity <> dw.Quantity

end

exec spUpdateFactSales