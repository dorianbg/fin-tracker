create or replace view total_return as
with prices as (
    select
        h.ticker,
        h.ticker_full,
        h."date" at time zone 'Europe/Paris' as date,
        h.close,
        h.volume,
        h.dividends,
        t.currency,
        t.description,
        t.fund_type
    from historical_prices h
    join ticker_ref t on h.ticker_full = t.ticker_full
    where h.ticker<>'EURGBP=X' and h.ticker<>'GBP=X' and h.volume > 0
), exchange_rates as (
    select date at time zone 'Europe/Paris' as date,
           case when ticker = 'EURGBP=X' then 'EUR'
                when ticker = 'GBP=X' then 'USD'
                else 'GBP'
               end as ccy,
            "close" as close
    from historical_prices
    where ticker='EURGBP=X' or ticker='GBP=X'
), prices_w_exchange_rate as (
        select prices.ticker,
                prices.ticker_full,
                prices."date",
                prices."close",
                prices.volume,
                prices.dividends,
                prices.currency,
                prices.description,
                prices.fund_type,
                coalesce(exchange_rates.ccy, 'GBP') as ccy2,coalesce(exchange_rates.close, 1) as ccy_conversion
        from prices
        ASOF LEFT JOIN exchange_rates
            on prices.currency = exchange_rates.ccy and prices.date >= exchange_rates.date
)
select ticker,
       ticker_full,
       "date",
       "close",
       "close" * ccy_conversion as price,
       volume,
--        dividends * ccy_conversion as dividend_conv,
       currency,
       'GBP' as currency_converted,
       description,
       fund_type
from prices_w_exchange_rate;


create or replace view instrument_annualised_volatility as
WITH log_returns AS (
    SELECT
        ticker_full,
        "date",
        "price",
        LN(h.price / LAG(h.price) OVER (PARTITION BY h.ticker ORDER BY date)) AS log_return
    FROM total_return h
    WHERE date between current_date - interval '1 year' and current_date
),
-- Step 2: Calculate Daily Volatility
daily_volatility AS (
    SELECT
        ticker_full,
        STDDEV_POP(log_return) AS daily_volatility
    FROM log_returns
    GROUP BY ticker_full
)
-- Step 3: Annualize Volatility
SELECT
    ticker_full,
    daily_volatility as vol_1d,
    daily_volatility * SQRT(252) AS vol_1y
FROM
    daily_volatility;


create or replace view latest_performance as
with stage1 as (
    select
        ticker,
        h.ticker_full,
        "date",
        "price",
        description,
        fund_type,
        "price" - (lag("price", 1, 0) over one_year) as day_price_diff,
        "price" - (lag("price", 5, 0) over one_year) as week_price_diff,
        "price" - (lag("price", 10, 0) over one_year) as two_week_price_diff,
        "price" - (lag("price", 15, 0) over one_year) as three_week_price_diff,
        "price" - (lag("price", 21, 0) over one_year) as month_price_diff,
        "price" - (lag("price", 63, 0) over one_year) as quarter_price_diff,
        "price" - (lag("price", 126, 0) over one_year) as half_year_price_diff,
        "price" - (lag("price", 252, 0) over one_year) as year_price_diff,
        "price" - (lag("price", 2*252, 0) over one_year) as two_year_price_diff,
        "price" - (lag("price", 3*252, 0) over one_year) as three_year_price_diff,
        "price" - (lag("price", 5*252, 0) over one_year) as five_year_price_diff
    from total_return h
    WINDOW
        one_year AS (
            PARTITION BY h.ticker_full
            ORDER BY "date" ASC
            RANGE BETWEEN INTERVAL 252 DAYS PRECEDING AND current row
        ),
        five_year AS (
            PARTITION BY h.ticker_full
            ORDER BY "date" ASC
            RANGE BETWEEN INTERVAL 1260 DAYS PRECEDING AND current row
        )
), stage2 as (
SELECT
    ticker,
    ticker_full,
    "date"::date as date,
    "date" as dt,
    row_number() over (partition by ticker order by date desc) as rown,
--     "price",
    replace(replace(replace(description, 'iShares ', ''), 'MSCI ', ''), 'SPDR® ', '')  as description,
    fund_type,
-- returns
    round(("price" / ("price" - day_price_diff) - 1) * 100, 2) as r_1d,
    round(("price" / ("price" - week_price_diff) - 1) * 100, 2) as r_1w,
    round(("price" / ("price" - two_week_price_diff) - 1) * 100, 2) as r_2w,
    round(("price" / ("price" - month_price_diff) - 1) * 100, 2) as r_1mo,
    round(("price" / ("price" - quarter_price_diff) - 1) * 100, 2) as r_3mo,
    round(("price" / ("price" - half_year_price_diff) - 1) * 100, 2) as r_6mo,
    round(("price" / ("price" - year_price_diff) - 1) * 100, 2) as r_1y,
    round(("price" / ("price" - year_price_diff) - 1) * 100, 2) as r_2y,
    round(("price" / ("price" - two_year_price_diff) - 1) * 100, 2) as r_3y,
    round(("price" / ("price" - five_year_price_diff) - 1) * 100, 2) as r_5y,
-- z-scores
    round((abs(day_price_diff) / (stddev_pop(day_price_diff) over one_month)), 2) as z_1d,
    round((abs(week_price_diff) / (stddev_pop(week_price_diff) over one_month)), 2) as z_1w,
    round(abs(two_week_price_diff) / (stddev_pop(two_week_price_diff) over one_month), 2) as z_2w,
    round(abs(month_price_diff) / (stddev_pop(month_price_diff) over one_month), 2) as z_1mo,
-- moving averages
    round(("price"/avg("price") over one_month - 1) * 100, 2) as px_21_dma,
    round(("price"/avg("price") over three_month - 1) * 100, 2) as px_63_dma,
    round(("price"/avg("price") over one_year - 1) * 100, 2) as px_252_dma
FROM stage1
WINDOW
    one_year AS (
        PARTITION BY ticker_full
        ORDER BY "date" ASC
        RANGE BETWEEN INTERVAL 252 DAYS PRECEDING AND current row
    ),
    one_month AS (
        PARTITION BY ticker_full
        ORDER BY "date" ASC
        RANGE BETWEEN INTERVAL 21 DAYS PRECEDING AND current row
    ),
    three_month AS (
            PARTITION BY ticker_full
            ORDER BY "date" ASC
            RANGE BETWEEN INTERVAL 63 DAYS PRECEDING AND current row
    )
)
select
    s.*,
    round(vol.vol_1d * 100, 2) as vol_1d,
    round(vol.vol_1y * 100, 2) as vol_1y
from stage2 s
left join instrument_annualised_volatility vol
    on s.ticker_full = vol.ticker_full
where rown = 1
-- and r_1w between -50 and 50
order by s.dt desc;