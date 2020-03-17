import datetime
BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
CONFIRMED = "time_series_19-covid-Confirmed.csv"
DEATH = "time_series_19-covid-Deaths.csv"
RECOVERED = "time_series_19-covid-Recovered.csv"
RAW_OUTPUT_CSV = "time-series-19-covid-combined-raw.csv"
ENHANCED_OUTPUT_CSV = "time-series-19-covid-combined-enhanced.csv"
COUNTIES_CSV = "data/county_geocodes.csv"
STATES_CSV = "data/state_geocodes.csv"
def get_todays_output_name():
    date = datetime.date.today().isoformat()
    todays_name = f"time-series-19-covid-combined-enhanced_{date}.csv" 
    return todays_name