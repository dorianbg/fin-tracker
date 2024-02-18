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

The application stores historical prices of instruments in a DuckDB database file. 

End of day prices are fetched from Yahoo finance API - but ingestion is incremental ie. we only fetch prices for missing data in the lookback period.

The `resources\instrument_info.csv` file contains a list of instruments for which historical data needs to be fetched (as per `lookback_period` in `fintracker/consts` file)

This script supports exporting data to a PostgreSQL database.
Use case for this was to allow visualisation using Grafana. 
For that you should just pass in the required arguments and set relevant environment variables. 

### Dev setup

Please refer to [this article](https://dorianbg.github.io/posts/python-project-setup-best-practice/).
After setting up pyenv and Poetry, it should be straight forward to run the script.
