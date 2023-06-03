"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table", partitions=1)
table = app.Table("org.chicago.cta.stations.table.v1", default=1, partitions=1, changelog_topic=out_topic,)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#

def get_line(event):
    if event.red:
        return "red"
    elif event.blue:
        return "blue"
    elif event.green:
        return "green"
    else:
        return ""

async def station_event(event):
    async for event in events:
        line = get_line(event)
        transformed_station = TransformedStation(
                station_id=event.station_id,
                station_name=event.station_name,
                order=event.order,
                line=line
            )
        table[transformed_station.station_id] = transformed_station
        



if __name__ == "__main__":
    app.main()
