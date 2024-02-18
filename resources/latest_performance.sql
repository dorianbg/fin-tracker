create or replace view latest_performance as
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
), total_ret as (
    select ticker,
           ticker_full,
           "date",
           "close",
           "close" * ccy_conversion as price_cumulative,
           volume,
--            dividends * ccy_conversion as dividend_conv,
           currency,
           description,
           fund_type
    from prices_w_exchange_rate
-- ), dividends_1 as (
--     select *, sum(dividend_conv) over (PARTITION BY ticker_full ORDER BY "date" ASC) as dividends_cumulative
--     from adjusted_prices ap
-- ), total_ret2 as (
--     select *, close_conv + dividends_cumulative as "price_cumulative"
--     from dividends_1
), stage1 as (
    select
        ticker,
        h.ticker_full,
        "date",
        "price_cumulative",
        description,
        "price_cumulative" - (lag("price_cumulative", 1, 0) over one_year) as day_price_diff,
        "price_cumulative" - (lag("price_cumulative", 5, 0) over one_year) as week_price_diff,
        "price_cumulative" - (lag("price_cumulative", 10, 0) over one_year) as two_week_price_diff,
        "price_cumulative" - (lag("price_cumulative", 15, 0) over one_year) as three_week_price_diff,
        "price_cumulative" - (lag("price_cumulative", 21, 0) over one_year) as month_price_diff,
        "price_cumulative" - (lag("price_cumulative", 63, 0) over one_year) as quarter_price_diff,
        "price_cumulative" - (lag("price_cumulative", 126, 0) over one_year) as half_year_price_diff,
        "price_cumulative" - (lag("price_cumulative", 252, 0) over one_year) as year_price_diff,
        "price_cumulative" - (lag("price_cumulative", 2*252, 0) over one_year) as two_year_price_diff,
        "price_cumulative" - (lag("price_cumulative", 3*252, 0) over one_year) as three_year_price_diff,
        "price_cumulative" - (lag("price_cumulative", 5*252, 0) over one_year) as five_year_price_diff,
    from total_ret h
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
    ticker as tckr,
    ticker_full,
    "date" as dt,
--     "price_cumulative",
    replace(replace(replace(description, 'iShares ', ''), 'MSCI ', ''), 'SPDR® ', '')  as description,

--     day_price_diff,
    round(("price_cumulative" / ("price_cumulative" - day_price_diff) - 1) * 100, 2) as r_1d,
--     stddev_pop(day_price_diff) over one_month as std_dev_day_price_diff,
    round((abs(day_price_diff) / (stddev_pop(day_price_diff) over one_month)), 2) as z_1d,

--     week_price_diff,
    round(("price_cumulative" / ("price_cumulative" - week_price_diff) - 1) * 100, 2) as r_1w,
--     stddev_pop(week_price_diff) over one_month as std_dev_week_price_diff,
    round((abs(week_price_diff) / (stddev_pop(week_price_diff) over one_month)), 2) as z_1w,

--     two_week_price_diff,
    round(("price_cumulative" / ("price_cumulative" - two_week_price_diff) - 1) * 100, 2) as r_2w,
--     stddev_pop(two_week_price_diff) over one_month as std_dev_two_week_price_diff,
    round(abs(two_week_price_diff) / (stddev_pop(two_week_price_diff) over one_month), 2) as z_2w,

--     three_week_price_diff,
--     ("price_cumulative" / ("price_cumulative" - three_week_price_diff) - 1) * 100 as return_3w,
--     stddev_pop(three_week_price_diff) over one_month as std_dev_three_week_price_diff,
--     abs(three_week_price_diff) / (stddev_pop(three_week_price_diff) over one_month) as z_score_price_move_3w,

--     month_price_diff,
    round(("price_cumulative" / ("price_cumulative" - month_price_diff) - 1) * 100, 2) as r_1mo,
--     stddev_pop(month_price_diff) over one_month as std_dev_month_price_diff,
    round(abs(month_price_diff) / (stddev_pop(month_price_diff) over one_month), 2) as z_1mo,

--     quarter_price_diff,
    round(("price_cumulative" / ("price_cumulative" - quarter_price_diff) - 1) * 100, 2) as r_3mo,
--     stddev_pop(quarter_price_diff) over one_month as std_dev_quarter_price_diff,
--     abs(quarter_price_diff) / (stddev_pop(quarter_price_diff) over one_month) as z_score_price_move_3mo,

--     half_year_price_diff,
    round(("price_cumulative" / ("price_cumulative" - half_year_price_diff) - 1) * 100, 2) as r_6mo,

--     year_price_diff,
    round(("price_cumulative" / ("price_cumulative" - year_price_diff) - 1) * 100, 2) as r_1y,

    round(("price_cumulative" / ("price_cumulative" - year_price_diff) - 1) * 100, 2) as r_2y,
    round(("price_cumulative" / ("price_cumulative" - two_year_price_diff) - 1) * 100, 2) as r_3y,
    round(("price_cumulative" / ("price_cumulative" - five_year_price_diff) - 1) * 100, 2) as r_5y,

    round(("price_cumulative"/avg("price_cumulative") over one_month - 1) * 100, 2) as px_20_dma,
    round(("price_cumulative"/avg("price_cumulative") over one_year - 1) * 100, 2) as px_252_dma
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
    )
)
select * from stage2 s
join (
    select max(date at time zone 'Europe/Paris') as dt, ticker_full
    from historical_prices h group by ticker_full
) d
    on s.dt::date = d.dt::date  and s.ticker_full = d.ticker_full
-- where r_1w between -50 and 50
order by s.dt desc;