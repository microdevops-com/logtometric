import time
import re
import logging
import asyncio
from datetime import datetime
from itertools import groupby
from statistics import pstdev, fmean
from copy import copy


logger = logging.getLogger(__name__)

class BaseLogItem:
    def __init__(self):
        self.items = []
        self.last_value = 0
        self.values = []
        self.timestamps = []
        self.last_calc_ts = time.time()
        self.log = logging.getLogger(self.__class__.__name__)

    async def defer(self):
        while time.time() - self.last_calc_ts < 1:
            self.log.debug(f"Deferring calculation")
            await asyncio.sleep(0.1)
        self.last_calt_ts = time.time()

    def append(self, item):
        self.items.append(item)

    def dump(self):
        return {"values": copy(self.values), "timestamps": copy(self.timestamps)}

    def clear(self):
        self.timestamps.clear()
        self.values.clear()

class LogItemCount(BaseLogItem):
    def __init__(self):
        super().__init__()

    async def calculate(self):
        await self.defer()
        if self.items:
            self.last_value += len(self.items)
            self.values.append(self.last_value)
            ts = round(time.time()) * 1000
            self.timestamps.append(ts)
            self.items.clear()
        else:
            self.values.append(self.last_value)
            self.timestamps.append(round(time.time())*1000)


class LogItemAvgSum(BaseLogItem):
    def __init__(self):
        super().__init__()
        self.timere = re.compile(r"(?P<ts>2\d{3}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")

    def append(self, item):
        timeline = self.timere.search(item["line"]).group("ts")
        ts = round(datetime.strptime(timeline, '%Y-%m-%d %H:%M:%S,%f').timestamp()) * 1000
        item["ts"] = ts
        self.items.append(item)

    async def calculate(self):
        await self.defer()
        if self.items:
            for ts, grp in groupby(self.items, key=lambda x: x["ts"]):
                grp = [*grp]
                ts = round(time.time()) * 1000
                values = [float(i["value"]) for i in grp]
                self.last_value += pstdev(values) + fmean(values)
                self.values.append(self.last_value)
                self.timestamps.append(ts)
            self.items.clear()
        else:
            self.values.append(self.last_value)
            self.timestamps.append(round(time.time())*1000)
