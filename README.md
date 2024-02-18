# Fin tracker


### Features
- Final output calculates:
  - returns on different time periods
  - Z scores of those returns to take into account volatility 
- Why it's useful:
  - performance is calculated in GBP (exchange rate performance taken into account for instruments priced in EUR or USD)
  - dividend adjustments into performance
  - you can redefine some of the metrics as per your requirements and implement more complicated aggregate scores
- Good practices:
  - extendable to track any reasonable number of instruments
  - idempotent with caching of expensive API call results

### How it works

The application uses a DuckDB instance to store historical prices of instruments. 

End of day prices are first fetched from Yahoo finance but only missing time ranges compared to existing data in the lookback period.

The `instrument_info.csv` file in resources folder contains a list of instruments for which historical data needs to be fetched (as per `lookback_period` in fintracker/consts file)

This script supports exports of data to a PostgreSQL database for visualisation in Grafana. For that pass in the required arguments and set relevant environment variables. 
