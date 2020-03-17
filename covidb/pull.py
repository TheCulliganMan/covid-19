from dataflows import (
    Flow,
    add_computed_field,
    delete_fields,
    dump_to_path,
    find_replace,
    join,
    load,
    set_type,
    unpivot,
    update_resource,
)
from covidb.config import BASE_URL, CONFIRMED, DEATH, RECOVERED, RAW_OUTPUT_CSV

def to_normal_date(row):
    month, day, year = row["Date"].split("-")
    day = f"0{day}" if len(day) == 1 else day
    month = f"0{month}" if len(month) == 1 else month
    row["Date"] = "-".join([day, month, year])


def data_pull_csv():
    unpivoting_fields = [{"name": r"([0-9]+\/[0-9]+\/[0-9]+)", "keys": {"Date": r"\1"}}]

    extra_keys = [{"name": "Date", "type": "string"}]
    extra_value = {"name": "Case", "type": "number"}

    Flow(
        load(f"{BASE_URL}{CONFIRMED}"),
        load(f"{BASE_URL}{RECOVERED}"),
        load(f"{BASE_URL}{DEATH}"),
        unpivot(unpivoting_fields, extra_keys, extra_value),
        find_replace([{"name": "Date", "patterns": [{"find": "/", "replace": "-"}]}]),
        to_normal_date,
        set_type("Date", type="date", format="%d-%m-%y", resources=None),
        set_type("Case", type="number", resources=None),
        join(
            source_name="time_series_19-covid-Confirmed",
            source_key=["Province/State", "Country/Region", "Date"],
            source_delete=True,
            target_name="time_series_19-covid-Deaths",
            target_key=["Province/State", "Country/Region", "Date"],
            fields=dict(Confirmed={"name": "Case", "aggregate": "first"}),
        ),
        join(
            source_name="time_series_19-covid-Recovered",
            source_key=["Province/State", "Country/Region", "Date"],
            source_delete=True,
            target_name="time_series_19-covid-Deaths",
            target_key=["Province/State", "Country/Region", "Date"],
            fields=dict(Recovered={"name": "Case", "aggregate": "first"}),
        ),
        add_computed_field(
            target={"name": "Deaths", "type": "number"},
            operation="format",
            with_="{Case}",
        ),
        delete_fields(["Case"]),
        update_resource(
            "time_series_19-covid-Deaths",
            name="time-series-19-covid-combined",
            path=RAW_OUTPUT_CSV,
        ),
        dump_to_path(),
    ).results()[0]


if __name__ == "__main__":
    data_pull_csv()
