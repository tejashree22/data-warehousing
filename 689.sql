--creating company_dimension table--

drop TABLE company_dimension

CREATE TABLE company_dimension (
    CompanyID SERIAL PRIMARY KEY, -- Assuming CompanyID should be an auto-incrementing integer
    IndustryID INT,
    Symbol TEXT,
    ShortName TEXT,
    LongName TEXT,
    Sector TEXT,
    Industry TEXT,
    Exchange TEXT,
    Headquarters TEXT,
    FulltimeEmployees INTEGER,
    LongBusinessSummary TEXT,
    FOREIGN KEY (IndustryID) REFERENCES Industry_Dimension(IndustryID)
);


INSERT INTO company_dimension(
	symbol,
	shortname, 
	longname,
	sector, 
	industry, 
	exchange, 
	headquarters, 
	fulltimeemployees, 
	longbusinesssummary)
SELECT  "Shortname", 
    "Symbol",
	"Longname", 
    "Sector", 
    "Industry", 
    "Exchange", 
    "City", 
    "Fulltimeemployees", 
    "Longbusinesssummary"
	FROM sp500_companies;
	
	
--verify--

select * from company_dimension


--creating Time_Dimension table--

CREATE TABLE Time_Dimension (
    TimeID SERIAL PRIMARY KEY,
    Date DATE NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    Quarter INT NOT NULL,
    WeekOfYear INT NOT NULL,
    DayOfWeek INT NOT NULL,
    MonthName TEXT NOT NULL,
    DayName TEXT NOT NULL,
    IsWeekend BOOLEAN NOT NULL
);

INSERT INTO Time_Dimension (Date, Year, Month, Day, Quarter, WeekOfYear, DayOfWeek, MonthName, DayName, IsWeekend)
SELECT
    date,
    EXTRACT(YEAR FROM date) AS Year,
    EXTRACT(MONTH FROM date) AS Month,
    EXTRACT(DAY FROM date) AS Day,
    EXTRACT(QUARTER FROM date) AS Quarter,
    EXTRACT(WEEK FROM date) AS WeekOfYear,
    EXTRACT(DOW FROM date) AS DayOfWeek,
    TO_CHAR(date, 'Month') AS MonthName,
    TO_CHAR(date, 'Day') AS DayName,
    CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS IsWeekend
FROM GENERATE_SERIES('2000-01-01'::DATE, '2030-12-31'::DATE, '1 day') AS date;

--verify--

select * from time_dimension


--creating Location_Dimension table--

CREATE TABLE Location_Dimension (
    LocationID SERIAL PRIMARY KEY,
    FIPSCode VARCHAR(10),
    StateArea VARCHAR(255),
    Country VARCHAR(255),
    Population INT,
    LaborForce INT,
    UnemploymentRate FLOAT,
    Date DATE
);

INSERT INTO Location_Dimension (FIPSCode, StateArea, Country, Population, LaborForce, UnemploymentRate, Date)
SELECT 
    "FIPS Code" AS FIPSCode,
    "State/Area" AS StateArea,
    'United States' AS Country, -- Replace with the actual country value or a placeholder
    "Total Civilian Non-Institutional Population in State/Area" AS Population,
    "Total Civilian Labor Force in State/Area" AS LaborForce,
    "Percent (%) of Labor Force Unemployed in State/Area" AS UnemploymentRate,
    TO_DATE("Date", 'YYYY-MM-DD') AS Date
FROM unemployment_data;

--verify--
select * from Location_Dimension

--creating Location_Dimension table--

CREATE TABLE state_labor_market_dimension (
    StateID SERIAL PRIMARY KEY,
    StateName VARCHAR(255),
    LaborForceSize INT,
    EmploymentLevel INT,
    UnemploymentLevel INT,
    UnemploymentRate DECIMAL(5,2),
    AvgLaborForce INT,
    AvgEmploymentLevel INT,
    AvgUnemploymentLevel INT,
    AvgUnemploymentRate DECIMAL(5,2)
);

-- Populate state_labor_market_dimension table from unemployment dataset
INSERT INTO state_labor_market_dimension (StateName, LaborForceSize, EmploymentLevel, UnemploymentLevel, UnemploymentRate)
SELECT 
    "State/Area" AS StateName,
    "Total Civilian Labor Force in State/Area" AS LaborForceSize,
    "Total Employment in State/Area" AS EmploymentLevel,
    "Total Unemployment in State/Area" AS UnemploymentLevel,
    "Percent (%) of Labor Force Unemployed in State/Area" AS UnemploymentRate
FROM unemployment_data;

-- First, calculate the average labor statistics for each state
WITH AggregatedData AS (
    SELECT
        "State/Area" AS StateName,
        AVG("Total Civilian Labor Force in State/Area") AS AvgLaborForce,
        AVG("Total Employment in State/Area") AS AvgEmploymentLevel,
        AVG("Total Unemployment in State/Area") AS AvgUnemploymentLevel,
        AVG("Percent (%) of Labor Force Unemployed in State/Area") AS AvgUnemploymentRate
    FROM unemployment_data
    GROUP BY "State/Area"
)

-- Update the state_labor_market_dimension table with the aggregated data
UPDATE state_labor_market_dimension
SET
    AvgLaborForce = AD.AvgLaborForce,
    AvgEmploymentLevel = AD.AvgEmploymentLevel,
    AvgUnemploymentLevel = AD.AvgUnemploymentLevel,
    AvgUnemploymentRate = AD.AvgUnemploymentRate
FROM AggregatedData AD
WHERE state_labor_market_dimension.StateName = AD.StateName;


--verify--
select * from state_labor_market_dimension

-- Update the company names to be consistently formatted with proper capitalization
UPDATE company_dimension
SET
    ShortName = INITCAP(TRIM(ShortName)),
    LongName = INITCAP(TRIM(LongName));
	
-- Create a view to store average and median values by sector
CREATE OR REPLACE VIEW sector_aggregates AS
SELECT 
    Sector,
    AVG(FulltimeEmployees) AS AvgEmployees,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY MarketCap) AS MedianMarketCap
FROM 
    company_dimension
GROUP BY 
    Sector;

-- create econimic sector dimension--
CREATE TABLE Economic_Sector_Dimension (
    SectorID SERIAL PRIMARY KEY,
    SectorName VARCHAR(255) UNIQUE NOT NULL,
    SectorDescription TEXT,
    SectorCode VARCHAR(100) UNIQUE,
    SectorImpact VARCHAR(255),
    PrimaryIndustries TEXT,
    RegulatoryBody VARCHAR(255),
    EconomicImpact TEXT,
    KeyCompanies TEXT,
    AverageMarketCap NUMERIC,
    AverageEBITDA NUMERIC,
    EmploymentImpact TEXT
);


-- Create a temporary view with aggregated data
CREATE TEMP VIEW sector_aggregates AS
SELECT 
    "Sector" AS SectorName,
    STRING_AGG("Shortname", ', ' ORDER BY "Marketcap" DESC) AS KeyCompanies,
    AVG("Marketcap") AS AverageMarketCap,
    AVG("Ebitda") AS AverageEBITDA,
    COUNT(*) AS CompaniesInSector,
    'SC-' || LEFT("Sector", 3) || '-' || MIN("Symbol") AS SectorCode,  -- Assuming 'Symbol' is used as part of a code
    'Primary industries for ' || "Sector" AS PrimaryIndustries,  -- Placeholder
    CASE 
        WHEN "Sector" = 'Technology' THEN 'Federal Communications Commission (FCC)'
        WHEN "Sector" = 'Healthcare' THEN 'Food and Drug Administration (FDA)'
        ELSE 'Generic Regulatory Body'
    END AS RegulatoryBody,
    'High volume transactions and major market player' AS EconomicImpact,  -- Placeholder
    'Significant employment provider in various regions' AS EmploymentImpact  -- Placeholder
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY "Sector" ORDER BY "Marketcap" DESC) AS rn
    FROM sp500_companies
) sub
WHERE sub.rn <= 5
GROUP BY 
    "Sector";

-- Insert aggregated data into Economic_Sector_Dimension
INSERT INTO Economic_Sector_Dimension (
    SectorName,
    KeyCompanies,
    AverageMarketCap,
    AverageEBITDA,
    SectorDescription,
    SectorImpact,
    SectorCode,
    PrimaryIndustries,
    RegulatoryBody,
    EconomicImpact,
    EmploymentImpact
)
SELECT 
    SectorName,
    KeyCompanies,
    AverageMarketCap,
    AverageEBITDA,
    'Description of ' || SectorName,
    CASE 
        WHEN AverageMarketCap > 1000000000000 THEN 'High Impact'
        WHEN AverageMarketCap BETWEEN 100000000000 AND 1000000000000 THEN 'Medium Impact'
        ELSE 'Low Impact'
    END AS SectorImpact,
    SectorCode,
    PrimaryIndustries,
    RegulatoryBody,
    EconomicImpact,
    EmploymentImpact
FROM 
    sector_aggregates;

--verify--
select * from Economic_Sector_Dimension

-- create Exchange_Dimension--

CREATE TABLE Exchange_Dimension (
    ExchangeID SERIAL PRIMARY KEY,
    ExchangeName VARCHAR(255) UNIQUE NOT NULL,
    ExchangeCode VARCHAR(100) UNIQUE,
    ExchangeLocation VARCHAR(255),
    EstablishedYear INT,
    ExchangeCEO VARCHAR(255),
    MarketCap BIGINT,
    NumberOfListedCompanies INT,
    WebsiteURL VARCHAR(255),
    ExchangeDescription TEXT
);

-- Insert unique exchanges into the Exchange_Dimension table
INSERT INTO Exchange_Dimension (
    ExchangeName,
    ExchangeCode,
    ExchangeLocation,
    EstablishedYear,
    ExchangeCEO,
    MarketCap,
    NumberOfListedCompanies,
    WebsiteURL,
    ExchangeDescription
)
SELECT DISTINCT
    "Exchange" AS ExchangeName,
    SUBSTRING("Exchange" FROM 1 FOR 3) || '_Code' AS ExchangeCode,  -- Placeholder for Exchange Code
    'NewYork' AS ExchangeLocation,  -- Placeholder for Location
    1993 AS EstablishedYear,  -- Placeholder for Established Year
    'Unknown CEO' AS ExchangeCEO,  -- Placeholder for CEO
    "Marketcap",  
    1 AS NumberOfListedCompanies,  -- No data available for Number of Listed Companies
    'http://www.example.com' AS WebsiteURL,  -- Placeholder for Website URL
    'No Description Available' AS ExchangeDescription  -- Placeholder for Description
FROM 
    sp500_companies
ON CONFLICT (ExchangeName) DO NOTHING;  -- To avoid inserting duplicates if rerun

--verify--
select * from Exchange_Dimension


--create Event_Dimension table--
CREATE TABLE Event_Dimension (
    EventID SERIAL PRIMARY KEY,
    EventDate DATE,
    EventName VARCHAR(255),
    EventType VARCHAR(255),
    EventDescription TEXT,
    Impact VARCHAR(255),  
    OpenPrice NUMERIC,
    ClosePrice NUMERIC,
    AbsoluteChange NUMERIC
);

-- Insert identified stock events into Event_Dimension with debugging
INSERT INTO Event_Dimension (
    EventName,
    EventType,
    EventDate,
    EventDescription,
    Impact,
    OpenPrice,
    ClosePrice,
    AbsoluteChange
)
SELECT
    "Symbol" AS EventName,
    'Significant Price Change' AS EventType,
    CAST("Date" AS DATE) AS EventDate,
    'A significant change in closing price compared to opening price.' AS EventDescription,
    CASE
        WHEN ABS("Close" - "Open") / "Open" > 0.05 THEN 'High Impact'
        WHEN ABS("Close" - "Open") / "Open" > 0.005 THEN 'Moderate Impact'  -- Adjusted to include more range
        ELSE 'Low Impact'
    END AS Impact,
    "Open" AS OpenPrice,
    "Close" AS ClosePrice,
    ABS("Close" - "Open") AS AbsoluteChange
FROM
    sp500_stocks
WHERE
    "Open" IS NOT NULL AND "Close" IS NOT NULL AND
    ABS("Close" - "Open") / "Open" > 0.005;  -- Lowered the threshold to 5%

--verify--
select * from Event_Dimension

--create risk dimension--
CREATE TABLE Risk_Factor_Dimension (
    RiskID SERIAL PRIMARY KEY,
	Symbol VARCHAR(50), 
    RiskType VARCHAR(255) NOT NULL,
    RiskDescription TEXT,
    RiskLevel VARCHAR(255),
    RelatedSector VARCHAR(255),
    ImpactDescription TEXT,
	Volatility NUMERIC
);


-- Insert calculated risk and volatility into Risk_Factor_Dimension
INSERT INTO Risk_Factor_Dimension (
    RiskType,
    RiskDescription,
    RiskLevel,
    RelatedSector,
    ImpactDescription,
    Volatility  -- Added the column to store the computed volatility
)
SELECT 
    'Volatility' AS RiskType,
    'Risk associated with high price fluctuations.' AS RiskDescription,
    CASE 
        WHEN STDDEV("Close" - "Open") / AVG("Close") > 0.02 THEN 'High'
        WHEN STDDEV("Close" - "Open") / AVG("Close") > 0.01 THEN 'Moderate'
        ELSE 'Low'
    END AS RiskLevel,
    (SELECT "Sector" FROM sp500_companies WHERE "Symbol" = sp500_stocks."Symbol" LIMIT 1) AS RelatedSector,
    'High volatility indicates higher risk but also potential for higher returns.' AS ImpactDescription,
    STDDEV("Close" - "Open") / AVG("Close") AS Volatility  -- Calculating volatility
FROM 
    sp500_stocks
GROUP BY 
    "Symbol";
	
UPDATE Risk_Factor_Dimension rd
SET RelatedSector = sc."Sector"
FROM sp500_stocks ss
JOIN sp500_companies sc ON ss."Symbol" = sc."Symbol"
WHERE rd.RiskType = 'Volatility'
AND rd.RiskLevel = ss."Symbol"; -- This line may need to be adjusted depending on your schema

SELECT rd.*, ss."Symbol"
FROM Risk_Factor_Dimension rd
JOIN sp500_stocks ss ON rd.riskid::text = ss."Symbol";

--verify--
select * from Risk_Factor_Dimension


-- Creating Heirarchy -- ETL process--

CREATE TABLE Sector_Dimension (
    SectorID SERIAL PRIMARY KEY,
    SectorName VARCHAR(255) UNIQUE NOT NULL,
    SectorDescription TEXT
);

CREATE TABLE Industry_Dimension (
    IndustryID SERIAL PRIMARY KEY,
    IndustryName VARCHAR(255) UNIQUE NOT NULL,
    SectorID INT,
    FOREIGN KEY (SectorID) REFERENCES Sector_Dimension(SectorID)
);



select * from Sector_Dimension

select * from Industry_Dimension

select * from company_dimension


-- Example of populating sectors (needs adjustment based on actual data source)
INSERT INTO Sector_Dimension (SectorName)
SELECT DISTINCT "Sector"
FROM sp500_companies 
ON CONFLICT (SectorName) DO NOTHING;

-- Example of populating industries linked to their sectors
INSERT INTO Industry_Dimension (IndustryName, SectorID)
SELECT DISTINCT "Industry", sd.SectorID
FROM sp500_companies 
JOIN Sector_Dimension sd ON sp500_companies."Sector" = sd.SectorName
ON CONFLICT (IndustryName) DO NOTHING;

-- Update companies with their respective industry IDs
UPDATE company_dimension AS cd
SET IndustryID = id.IndustryID
FROM sp500_companies AS sc
JOIN Industry_Dimension AS id ON sc."Industry" = id.IndustryName
WHERE cd."longname" = sc."Longname"; 


-- Query to check the updated results
SELECT cd.CompanyID, cd."longname", id.IndustryName, cd.IndustryID
FROM company_dimension AS cd
JOIN Industry_Dimension AS id ON cd.IndustryID = id.IndustryID
LIMIT 10;  


-- Count records before and after update
SELECT COUNT(*) FROM company_dimension WHERE IndustryID IS NOT NULL;


-- Find any records without an IndustryID
SELECT CompanyID, "longname" FROM company_dimension WHERE IndustryID IS NULL;

-- Identify unmatched industries from sp500_companies
SELECT DISTINCT sc."Industry"
FROM sp500_companies sc
LEFT JOIN Industry_Dimension id ON sc."Industry" = id.IndustryName
WHERE id.IndustryID IS NULL;



-- Assuming 'LongName' matches 'ShortName' and data is now cleaned
UPDATE company_dimension AS cd
SET IndustryID = id.IndustryID
FROM sp500_companies AS sc
JOIN Industry_Dimension AS id ON TRIM(UPPER(sc."Industry")) = TRIM(UPPER(id.IndustryName))
WHERE TRIM(UPPER(cd."longname")) = TRIM(UPPER( sc."Longname" ));


select * from company_dimension

-- Analysis--

SELECT
    sd.SectorName,
    id.IndustryName,
    COUNT(cd.CompanyID) AS NumberOfCompanies,
    AVG(cd.FulltimeEmployees) AS AverageEmployees
FROM
    company_dimension AS cd
JOIN
    Industry_Dimension AS id ON cd.IndustryID = id.IndustryID
JOIN
    Sector_Dimension AS sd ON id.SectorID = sd.SectorID
GROUP BY
    sd.SectorName, id.IndustryName
ORDER BY
    sd.SectorName, id.IndustryName;


-- SCD types--

--SCD type1--

UPDATE company_dimension
SET headquarters = 'New Headquarters'
WHERE companyid = 1555;

select * from company_dimension

--SCD type2--

ALTER TABLE event_dimension
ADD COLUMN ValidFrom DATE,
ADD COLUMN ValidTo DATE,
ADD COLUMN IsCurrent BOOLEAN DEFAULT TRUE;

UPDATE Event_Dimension
SET ValidFrom = CURRENT_DATE,  
    ValidTo = '2025-12-31',    -- A common practice for "ongoing" records
    IsCurrent = TRUE;          -- All existing records are currently valid

BEGIN;

-- Update the current record to set its ValidTo date, marking it as no longer current
UPDATE Event_Dimension
SET ValidTo = CURRENT_DATE,  -- This sets the end date to today's date
    IsCurrent = FALSE        -- This marks the record as not the current version
WHERE EventID = 1 AND IsCurrent = TRUE;  -- Change the EventID to the ID of the event being updated

-- Insert a new record with the updated details
INSERT INTO Event_Dimension (
    EventName, EventType, EventDate, EventDescription, Impact, OpenPrice, ClosePrice, AbsoluteChange, ValidFrom, ValidTo, IsCurrent
)
VALUES (
    'AKK',  -- Updated Event Name
    'Significant Price Change',  -- Updated Event Type, if it's the same, no need to change
    '2024-04-25',  -- The date of the event, change if necessary
    'A significant change in closing price compared to opening price',  -- Updated Description
    'High',  -- The impact level, change if necessary
    120,  -- Updated open price, change if necessary
    110,  -- Updated close price, change if necessary
    10,   -- Updated absolute change, change if necessary
    CURRENT_DATE,  -- The date this new version becomes valid
    '2025-12-31',  -- Future end date or a date very far in the future
    TRUE   -- This marks the record as the current version
);

COMMIT;


--verify--
select * from event_dimension

--Fact Tables--

--creating stock_performance_fact--
CREATE TABLE stock_performance_fact (
    FactID SERIAL PRIMARY KEY,
    Symbol TEXT,
    LongName TEXT,
    Date DATE,
    OpeningPrice NUMERIC,
    ClosingPrice NUMERIC,
    PriceChange NUMERIC,
    PriceChangePercentage NUMERIC
);




INSERT INTO stock_performance_fact (
    Symbol,
    LongName,
    Date,
    OpeningPrice,
    ClosingPrice,
    PriceChange,
    PriceChangePercentage
)

SELECT
  sc."Symbol" as Symbol ,
  cd.longname as LongName,
  td.date as Date,
  ss."Open"  as OpeningPrice,
  ss."Close" as ClosingPrice,
  (ss."Close" - ss."Open") AS "PriceChange",
  ROUND(((ss."Close" - ss."Open") / ss."Open")::NUMERIC * 100, 2) AS "PriceChangePercentage"
FROM
  sp500_companies AS sc
JOIN
  company_dimension AS cd ON sc."Symbol" = cd."shortname"
JOIN
  sp500_stocks AS ss ON sc."Symbol" = ss."Symbol"
JOIN
  time_dimension AS td ON ss."Date"::DATE = td.date
WHERE
  td.date = ss."Date"::DATE
ORDER BY
  td.date, sc."Symbol";

-- adding column--
--ALTER TABLE stock_performance_fact
--ADD COLUMN Volume BIGINT;



UPDATE stock_performance_fact spf
SET Volume = s."Volume"
FROM sp500_stocks s
WHERE spf.Symbol = s."Symbol" AND spf.Date = s."Date"::DATE;

--ALTER TABLE stock_performance_fact
--ADD COLUMN AverageDailyVolume BIGINT;

UPDATE stock_performance_fact spf
SET AverageDailyVolume = avg_volume.AverageDailyVolume
FROM (
    SELECT 
        cd.symbol,
        AVG(spf.Volume) as AverageDailyVolume
    FROM 
        stock_performance_fact spf
    INNER JOIN 
        company_dimension cd ON spf.symbol = cd.shortname -- Make sure this condition is correct for your schema
    GROUP BY 
        cd.symbol
) AS avg_volume
WHERE 
    spf.symbol = avg_volume.symbol;


-- Test update with a manual value
UPDATE stock_performance_fact
SET AverageDailyVolume = 100
WHERE EXISTS (
    SELECT 1
    FROM company_dimension
    WHERE stock_performance_fact.symbol = company_dimension.shortname
);

WITH VWAP_Calculation AS (
    SELECT 
        symbol,
        SUM(Volume * Openingprice) / SUM(Volume) AS VWAP  -- Calculate VWAP
    FROM 
        stock_performance_fact
    GROUP BY 
        symbol
)
SELECT * FROM VWAP_Calculation;


ALTER TABLE stock_performance_fact
ADD COLUMN VWAP NUMERIC;

WITH VWAP_Calculation AS (
    SELECT 
        symbol,
        SUM(Volume * Openingprice) / SUM(Volume) AS VWAP
    FROM 
        stock_performance_fact
    GROUP BY 
        symbol
)
UPDATE stock_performance_fact
SET VWAP = v.VWAP
FROM VWAP_Calculation v
WHERE stock_performance_fact.symbol = v.symbol;

--verify--
select * from stock_performance_fact




--fact 2--

--create Company_Unemployment_Fact--
CREATE TABLE Company_Unemployment_Fact (
    FactID SERIAL PRIMARY KEY,
    CompanyID INT,
    StateID INT,
    MarketCap BIGINT,
    Sector VARCHAR(255),
    UnemploymentRate NUMERIC(5, 2),
    FOREIGN KEY (CompanyID) REFERENCES company_dimension(CompanyID),
    FOREIGN KEY (StateID) REFERENCES location_dimension(LocationID)
);

INSERT INTO Company_Unemployment_Fact (CompanyID, StateID, MarketCap, Sector, UnemploymentRate)
SELECT 
    cd.companyid,
    ld.locationid,
    sc."Marketcap",
    sc."Sector",
    ud."Percent (%) of Labor Force Unemployed in State/Area" as UnemploymentRate
FROM 
    sp500_companies sc
JOIN 
    company_dimension cd ON sc."Symbol" = cd.shortname
JOIN 
    location_dimension ld ON sc."City" = ld.statearea
JOIN 
    unemployment_data ud ON ld.statearea = ud."State/Area"
WHERE
   ud."Date" = '1976-01-01' -- Assuming there is a date field to match a specific report date in unemployment_data
;

ALTER TABLE Company_Unemployment_Fact
ADD COLUMN RevenueGrowth NUMERIC(5, 2),
ADD COLUMN UnemploymentRateBucket NUMERIC(3, 1),
ADD COLUMN AverageRevenueGrowth NUMERIC(10, 2);



UPDATE Company_Unemployment_Fact f
SET RevenueGrowth = sc."Revenuegrowth"
FROM sp500_companies sc
JOIN company_dimension cd ON sc."Symbol" = cd.shortname
WHERE f.CompanyID = cd.companyid;

--

INSERT INTO Company_Unemployment_Fact (CompanyID, StateID, MarketCap, UnemploymentRate)
SELECT 
    cd.CompanyID,
    ld.LocationID,
    cuf.MarketCap,
    cuf.UnemploymentRate
FROM 
    Company_Unemployment_Fact cuf
JOIN 
    company_dimension cd ON cuf.CompanyID = cd.CompanyID
JOIN 
    location_dimension ld ON cuf.StateID = ld.LocationID
ORDER BY 
    cuf.UnemploymentRate DESC, cuf.MarketCap DESC;


--Correlation Between Revenue Growth and Unemployment Rate--

INSERT INTO Company_Unemployment_Fact(UnemploymentRateBucket, AverageRevenueGrowth)
SELECT 
    ROUND(UnemploymentRate, 1) AS UnemploymentRateBucket,
    AVG(RevenueGrowth) AS AverageRevenueGrowth
FROM 
    Company_Unemployment_Fact
GROUP BY 
    ROUND(UnemploymentRate, 1)
ORDER BY 
    AverageRevenueGrowth DESC;
	
	
	

select * from stock_performance_fact

select * from company_dimension

select * from time_dimension

select * from sector_dimension

select * from event_dimension

select * from exchange_dimension

select * from industry_dimension

select * from sp500_stocks 

select * from sp500_companies

select * from economic_sector_dimension

select * from unemployment_data

select * from Economic_impact_fact

select * from Company_Unemployment_Fact

select * from location_dimension

