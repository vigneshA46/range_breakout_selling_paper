import time
from datetime import datetime
from dhanhq import marketfeed
import os
from dotenv import load_dotenv
from datetime import datetime, date

load_dotenv()


# ============================
# CONFIG
# ============================
CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

NIFTY_SECURITY_ID = "25"


# ============================
# CANDLE BUILDER
# ============================
class OneMinuteCandleBuilder:
    def __init__(self):
        self.current_candle = None
        self.last_candle_time = None

    def update(self, tick):
        try:
            ltp = float(tick["LTP"])
            ltt = tick["LTT"]
        except:
            return None

        tick_time = datetime.combine(
            date.today(),
            datetime.strptime(ltt, "%H:%M:%S").time()
        )

        candle_time = tick_time.replace(second=0, microsecond=0)

        # ---- Ignore old ticks ----
        if self.last_candle_time and candle_time < self.last_candle_time:
            return None

        # ---- New candle ----
        if self.current_candle is None or self.current_candle["time"] != candle_time:
            finished = self.current_candle

            self.current_candle = {
                "time": candle_time,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp
            }

            self.last_candle_time = candle_time
            return finished

        # ---- Update ----
        self.current_candle["high"] = max(self.current_candle["high"], ltp)
        self.current_candle["low"] = min(self.current_candle["low"], ltp)
        self.current_candle["close"] = ltp

        return None

# ============================
# LIVE FEED
# ============================
def start():
    print("🚀 Live 1-Min Candle Builder Started")

    feed = marketfeed.DhanFeed(
        CLIENT_ID,
        ACCESS_TOKEN,
        [(marketfeed.IDX, NIFTY_SECURITY_ID, marketfeed.Quote)],
        version="v2"
    )

    candle_builder = OneMinuteCandleBuilder()

    while True:
        feed.run_forever()
        data = feed.get_data()
        print(data)

        if not data:
            continue

        # Sometimes dhan sends list / dict
        if isinstance(data, list):
            for tick in data:
                process_tick(tick, candle_builder)
        else:
            process_tick(data, candle_builder)
 

def process_tick(tick, candle_builder):
    if tick.get("type") != "Quote Data":
        return

    candle = candle_builder.update(tick)

    if candle:
        print(
            f"🕐 {candle['time']} | "
            f"O:{candle['open']} "
            f"H:{candle['high']} "
            f"L:{candle['low']} "
            f"C:{candle['close']}"
        )


# ============================
# RUN
# ============================
if __name__ == "__main__":
    start()
