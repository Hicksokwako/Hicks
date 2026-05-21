import asyncio
import websockets
import json
import csv
import os
from datetime import datetime
from random import shuffle
import aiohttp

# ----------- CONFIGURATION -----------
API_TOKEN = "YOUR_DERIV_API_TOKEN_HERE"
TELEGRAM_TOKEN = ""   # Optional: Telegram alerts
TELEGRAM_CHAT_ID = "" # Optional
DISCORD_WEBHOOK_URL = ""  # Optional

# Core trading parameters
MARKETS = [
    {"symbol": "R_100",   "name": "Volatility 100", "strategy": "RSI"},
    {"symbol": "R_50",    "name": "Volatility 50",  "strategy": "EMA_CROSS"},
    {"symbol": "R_25",    "name": "Volatility 25",  "strategy": "MACD"},
]

BASE_AMOUNT = 1
MARTI_MULT = 2
DURATION = 5
DURATION_UNIT = "t"
MAX_LOSS_STREAK = 5
MAX_STAKE = 32
AUTO_RECOVERY_DELAY = 90

# Indicator parameters
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
EMA_FAST = 5
EMA_SLOW = 10
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

LOG_FILE = "advanced_trade_log.csv"



# ----------- INDICATORS -----------
def calc_rsi(prices, period):
    if len(prices) < period + 1: return None
    gains, losses = [], []
    for i in range(1, period + 1):
        chg = prices[i] - prices[i-1]
        gains.append(chg if chg > 0 else 0)
        losses.append(abs(chg) if chg < 0 else 0)
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0: return 100
    rs = avg_gain / avg_loss if avg_loss else 0
    return 100 - (100 / (1 + rs))

def calc_ema(prices, period):
    if len(prices) < period: return None
    k = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = (p - ema) * k + ema
    return ema

def calc_ema_series(prices, period):
    emas = []
    if len(prices) < period: return emas
    k = 2 / (period + 1)
    ema = prices[0]
    for p in prices:
        ema = (p - ema) * k + ema
        emas.append(ema)
    return emas

def calc_macd(prices, fast, slow, sig):
    if len(prices) < slow + sig: return None, None, None
    ema_fast = calc_ema_series(prices, fast)
    ema_slow = calc_ema_series(prices, slow)
    macd_line = []
    for i in range(len(prices)):
        if i < slow - 1:
            macd_line.append(0)
        else:
            macd_line.append(ema_fast[i] - ema_slow[i])
    signal_line = calc_ema_series(macd_line, sig)
    hist = [macd_line[i] - signal_line[i] for i in range(len(signal_line))]
    return macd_line[-1], signal_line[-1], hist[-1]  # latest values

# ----------- ALERTERS -----------
async def telegram_notify(msg):
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID): return
    async with aiohttp.ClientSession() as session:
        await session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                           data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

async def discord_notify(msg):
    if not DISCORD_WEBHOOK_URL: return
    async with aiohttp.ClientSession() as session:
        await session.post(DISCORD_WEBHOOK_URL, json={"content": msg})


# ----------- BOT CLASS -----------
class ModularMartingaleBot:
    ENDPOINT = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    def __init__(self):
        self.connection = None
        self.amount = BASE_AMOUNT
        self.loss_streak = 0

        if not os.path.isfile(LOG_FILE):
            with open(LOG_FILE, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["datetime", "symbol", "market", "strategy", "indicator_value", "signal", "stake", "result", "profit"])

    async def connect(self):
        self.connection = await websockets.connect(self.ENDPOINT)
        await self.send({"authorize": API_TOKEN})
        print("Authorized with Deriv.")

    async def send(self, data):
        await self.connection.send(json.dumps(data))

    async def receive(self):
        msg = await self.connection.recv()
        return json.loads(msg)

    async def get_ticks(self, symbol, count):
        req = {"ticks_history": symbol, "adjust_start_time": 1, "count": count, "end": "latest", "style": "ticks"}
        await self.send(req)
        msg = await self.receive()
        if 'history' in msg and 'prices' in msg['history']:
            return msg['history']['prices']
        return []

    async def pick_signal(self, mkt):
        prices = []
        sig = None
        val = None
        strat = mkt.get("strategy","RSI").upper()

        # RSI
        if strat == "RSI":
            prices = await self.get_ticks(mkt["symbol"], RSI_PERIOD + 1)
            val = calc_rsi(prices, RSI_PERIOD)
            if val is not None:
                if val < RSI_OVERSOLD: sig = "CALL"
                elif val > RSI_OVERBOUGHT: sig = "PUT"
            return sig, val, "RSI"
        # EMA Crossover
        elif strat == "EMA_CROSS":
            need = max(EMA_FAST, EMA_SLOW) + 1
            prices = await self.get_ticks(mkt["symbol"], need)
            ema_fast = calc_ema(prices, EMA_FAST)
            ema_slow = calc_ema(prices, EMA_SLOW)
            if ema_fast and ema_slow:
                if ema_fast > ema_slow: sig = "CALL"
                elif ema_fast < ema_slow: sig = "PUT"
            val = ema_fast - ema_slow if ema_fast and ema_slow else None
            return sig, val, f"EMA({EMA_FAST})/EMA({EMA_SLOW})"
        # MACD
        elif strat == "MACD":
            need = MACD_SLOW + MACD_SIGNAL + 1
            prices = await self.get_ticks(mkt["symbol"], need)
            macd, signal_line, hist = calc_macd(prices, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            if macd and signal_line:
                if macd > signal_line: sig = "CALL"
                elif macd < signal_line: sig = "PUT"
            val = macd - signal_line if macd and signal_line else None
            return sig, val, f"MACD({MACD_FAST},{MACD_SLOW},{MACD_SIGNAL})"
        # Extend: add more strategies!
        else:
            return None, None, "NONE"

    def log_result(self, symbol, market, strategy, indicator_val, signal, stake, result, profit):
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.utcnow().isoformat(),
                symbol, market, strategy,
                f"{indicator_val:.4f}" if indicator_val is not None else "NA",
                signal or "NO_TRADE", stake, result, profit
            ])

    async def auto_recover(self):
        txt = f"[AUTO-RECOVERY] Cooldown... (delay {AUTO_RECOVERY_DELAY}s)."
        print(txt)
        await telegram_notify(txt)
        await discord_notify(txt)
        await asyncio.sleep(AUTO_RECOVERY_DELAY)
        self.loss_streak = 0
        self.amount = BASE_AMOUNT
        msg = "[BOT] Resuming with base stake after auto-recovery."
        print(msg)
        await telegram_notify(msg)
        await discord_notify(msg)

    async def run(self):
        await self.connect()
        await telegram_notify("[BOT] Modular Martingale Bot Started.")
        await discord_notify("[BOT] Modular Martingale Bot Started.")
        shuffle(MARKETS)
        while True:
            for mkt in MARKETS:
                signal, ind_val, stratname = await self.pick_signal(mkt)
                market = mkt.get("name", mkt["symbol"])
                if not signal:
                    print(f"[{market}][{stratname}] No signal (value: {ind_val})")
                    continue

                # Safety checks
                if self.loss_streak >= MAX_LOSS_STREAK or self.amount > MAX_STAKE:
                    await self.auto_recover()
                    break

                proposal = {
                    "buy": 1,
                    "parameters": {
                        "amount": self.amount,
                        "basis": "stake",
                        "contract_type": signal,
                        "currency": "USD",
                        "duration": DURATION,
                        "duration_unit": DURATION_UNIT,
                        "symbol": mkt["symbol"]
                    },
                    "price": self.amount
                }
                await self.send(proposal)
                msg = f"Trade: {market} | {stratname}={ind_val:.2f if ind_val else 'NA'} | {signal} | Stake={self.amount}"
                print(msg)
                await telegram_notify(msg)
                await discord_notify(msg)
                await self.handle_result(mkt["symbol"], market, stratname, ind_val, signal)
            await asyncio.sleep(4)

    async def handle_result(self, symbol, market, stratname, indicator_val, signal):
        while True:
            msg = await self.receive()
            if msg.get('msg_type') == 'buy':
                print(f"[{market}] Bought; awaiting result...")
            elif msg.get('msg_type') == 'proposal_open_contract':
                poc = msg['proposal_open_contract']
                if poc.get('is_sold'):
                    profit_loss = poc['profit']
                    result = "WIN" if profit_loss > 0 else "LOSS"
                    print(f">> [{market}] {signal}: {result} | PnL: {profit_loss}")
                    self.log_result(symbol, market, stratname, indicator_val, signal, self.amount, result, profit_loss)
                    await telegram_notify(f"Result: {market} | {signal} | Stake={self.amount} | {result} | PnL: {profit_loss}")
                    await discord_notify(f"Result: {market} | {signal} | Stake={self.amount} | {result} | PnL: {profit_loss}")
                    if profit_loss < 0:
                        self.loss_streak += 1
                        self.amount *= MARTI_MULT
                        print(f"LOSS: Streak={self.loss_streak} Next={self.amount}")
                    else:
                        self.loss_streak = 0
                        self.amount = BASE_AMOUNT
                        print("WIN: Resetting stake/streak")
                    await asyncio.sleep(2)
                    return

# --------- ENTRY POINT ---------
if __name__ == "__main__":
    bot = ModularMartingaleBot()
    asyncio.get_event_loop().run_until_complete(bot.run())
