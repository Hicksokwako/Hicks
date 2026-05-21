import asyncio
import websockets
import json
import csv
import os
from datetime import datetime
from collections import deque
import aiohttp
import streamlit as st
import threading
import time

# ----------- CONFIGURATION -----------
API_TOKEN = "YOUR_DERIV_API_TOKEN_HERE"

# Alerts (Telegram/Discord/etc can be filled later for full pro version)
TELEGRAM_TOKEN = ""
TELEGRAM_CHAT_ID = ""
DISCORD_WEBHOOK_URL = ""

# Core trading parameters
MARKETS = [
    {"symbol": "R_100",   "name": "Volatility 100", "strategy": "RSI"},
    {"symbol": "R_50",    "name": "Volatility 50",  "strategy": "EMA_CROSS"},
    {"symbol": "R_25",    "name": "Volatility 25",  "strategy": "MACD"},
    {"symbol": "R_10",    "name": "Volatility 10",  "strategy": "BOLLINGER"},
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
BB_PERIOD = 20
BB_STD = 2
ADX_PERIOD = 14
ADX_MIN = 18        # Minimum trending strength for filter

LOG_FILE = "pro_advanced_trade_log.csv"
SIGNAL_FILE = "signal.txt"  # File for external signal

DASHBOARD_PORT = 8501

# Serialize for dashboard
dashboard_state = {
    'current_market': '-',
    'current_strategy': '-',
    'current_signal': '-',
    'indicator_value': '-',
    'stake': 0,
    'result': '-',
    'profit': 0,
    'loss_streak': 0,
    'trade_log': deque(maxlen=100),
    'bot_status': 'IDLE'
}


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
    return macd_line[-1], signal_line[-1], hist[-1]

def calc_bollinger(prices, period=20, std=2):
    if len(prices) < period: return None, None, None
    window = prices[-period:]
    sma = sum(window) / period
    variance = sum((x - sma)**2 for x in window) / period
    stddev = variance ** 0.5
    upper = sma + std * stddev
    lower = sma - std * stddev
    return upper, sma, lower

def calc_adx(prices, period=14):
    # Simple ADX - use close prices only for this demo
    if len(prices) < period + 1: return None
    # Not true range or +DM/-DM, just a placeholder for filter effect
    diffs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(diffs[-period:]) / period

async def telegram_notify(msg):
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID): return
    async with aiohttp.ClientSession() as session:
        await session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                           data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

async def discord_notify(msg):
    if not DISCORD_WEBHOOK_URL: return
    async with aiohttp.ClientSession() as session:
        await session.post(DISCORD_WEBHOOK_URL, json={"content": msg})

class ProModularBot:
    ENDPOINT = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    def __init__(self):
        self.connection = None
        self.amount = BASE_AMOUNT
        self.loss_streak = 0
        self.pnl = 0
        self.running = False
        if not os.path.isfile(LOG_FILE):
            with open(LOG_FILE, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["datetime", "symbol", "market", "strategy", "indicator", "signal", "stake", "result", "profit", "cum_pnl"])
    async def connect(self):
        self.connection = await websockets.connect(self.ENDPOINT)
        await self.send({"authorize": API_TOKEN})
        dashboard_state['bot_status'] = 'CONNECTED'
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
    def external_file_signal(self):
        if os.path.isfile(SIGNAL_FILE):
            with open(SIGNAL_FILE) as f:
                s = f.read().strip().upper()
            return s if s in ['CALL', 'PUT'] else None
        return None
    async def pick_signal(self, mkt):
        prices = []
        sig = None
        val = None
        strat = mkt.get("strategy", "RSI").upper()
        adx_pass = True
        # ADX trend filter
        adx_val = None
        if ADX_MIN:
            prices_adx = await self.get_ticks(mkt["symbol"], ADX_PERIOD + 1)
            adx_val = calc_adx(prices_adx, ADX_PERIOD)
            adx_pass = adx_val is not None and adx_val >= ADX_MIN
        # External signal override
        ext_sig = self.external_file_signal()
        if ext_sig:
            return ext_sig, 'FILE', f'Ext:{ext_sig}', adx_pass, adx_val
        # RSI
        if strat == "RSI":
            prices = await self.get_ticks(mkt["symbol"], RSI_PERIOD + 1)
            val = calc_rsi(prices, RSI_PERIOD)
            if val is not None:
                if val < RSI_OVERSOLD: sig = "CALL"
                elif val > RSI_OVERBOUGHT: sig = "PUT"
            return sig, val, "RSI", adx_pass, adx_val
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
            return sig, val, f"EMA({EMA_FAST})/EMA({EMA_SLOW})", adx_pass, adx_val
        # MACD
        elif strat == "MACD":
            need = MACD_SLOW + MACD_SIGNAL + 1
            prices = await self.get_ticks(mkt["symbol"], need)
            macd, signal_line, hist = calc_macd(prices, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
            if macd and signal_line:
                if macd > signal_line: sig = "CALL"
                elif macd < signal_line: sig = "PUT"
            val = macd - signal_line if macd and signal_line else None
            return sig, val, f"MACD({MACD_FAST},{MACD_SLOW},{MACD_SIGNAL})", adx_pass, adx_val
        # Bollinger Bands
        elif strat == "BOLLINGER":
            prices = await self.get_ticks(mkt["symbol"], BB_PERIOD + 2)
            upper, sma, lower = calc_bollinger(prices, BB_PERIOD, BB_STD)
            if not upper: return None, None, "BBANDS", adx_pass, adx_val
            if prices[-2] <= lower and prices[-1] > lower:
                sig = "CALL"
            elif prices[-2] >= upper and prices[-1] < upper:
                sig = "PUT"
            val = (upper, sma, lower)
            return sig, val, f"BB({BB_PERIOD},{BB_STD})", adx_pass, adx_val
        else:
            return None, None, "NONE", adx_pass, adx_val
    def log_result(self, symbol, market, strategy, indicator_val, signal, stake, result, profit, pnl):
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.utcnow().isoformat(),
                symbol, market, strategy,
                f"{indicator_val}" if indicator_val else "NA",
                signal or "NO_TRADE", stake, result, profit, pnl
            ])
        dashboard_state['trade_log'].appendleft({
            'datetime': datetime.utcnow().strftime('%d-%H:%M:%S'),
            'symbol': symbol,
            'market': market,
            'strategy': strategy,
            'indicator_value': indicator_val,
            'signal': signal,
            'stake': stake,
            'result': result,
            'profit': profit,
            'cum_pnl': pnl
        })
    async def auto_recover(self):
        txt = f"[AUTO-RECOVERY] Cooldown... (delay {AUTO_RECOVERY_DELAY}s)."
        print(txt)
        dashboard_state['bot_status'] = f"{txt}"
        await telegram_notify(txt)
        await discord_notify(txt)
        await asyncio.sleep(AUTO_RECOVERY_DELAY)
        self.loss_streak = 0
        self.amount = BASE_AMOUNT
        msg = "[BOT] Resuming with base stake after auto-recovery."
        print(msg)
        dashboard_state['bot_status'] = msg
        await telegram_notify(msg)
        await discord_notify(msg)
    async def run(self):
        await self.connect()
        dashboard_state['bot_status'] = 'RUNNING'
        await telegram_notify("[BOT] Pro Modular Bot Started.")
        await discord_notify("[BOT] Pro Modular Bot Started.")
        while True:
            for mkt in MARKETS:
                signal, ind_val, stratname, adx_ok, adx_val = await self.pick_signal(mkt)
                market = mkt.get("name", mkt["symbol"])
                dashboard_state['current_market'] = market
                dashboard_state['current_strategy'] = stratname
                dashboard_state['current_signal'] = signal
                dashboard_state['indicator_value'] = ind_val
                dashboard_state['stake'] = self.amount
                dashboard_state['loss_streak'] = self.loss_streak
                dashboard_state['bot_status'] = f"RUNNING" if self.running else "WAITING"
                # ADX filter
                if not adx_ok:
                    dashboard_state['bot_status'] = f"Waiting (ADX low {adx_val})"
                    continue
                # No trade if no signal
                if not signal:
                    continue
                if self.loss_streak >= MAX_LOSS_STREAK or self.amount > MAX_STAKE:
                    await self.auto_recover()
                    break
                # Place trade
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
                msg = f"Trade: {market} | {stratname} | {signal} | Stake={self.amount}"
                print(msg)
                dashboard_state['bot_status'] = msg
                await telegram_notify(msg)
                await discord_notify(msg)
                await self.handle_result(mkt["symbol"], market, stratname, ind_val, signal)
            await asyncio.sleep(4)
    async def handle_result(self, symbol, market, stratname, indicator_val, signal):
        while True:
            msg = await self.receive()
            if msg.get('msg_type') == 'buy':
                print(f"[{market}] Bought; awaiting result...")
                dashboard_state['bot_status'] = f"Trade sent: {market}"
            elif msg.get('msg_type') == 'proposal_open_contract':
                poc = msg['proposal_open_contract']
                if poc.get('is_sold'):
                    profit_loss = poc['profit']
                    self.pnl += profit_loss
                    result = "WIN" if profit_loss > 0 else "LOSS"
                    dashboard_state['result'] = result
                    dashboard_state['profit'] = profit_loss
                    dashboard_state['bot_status'] = f"Last result: {result} PnL: {self.pnl}"
                    self.log_result(symbol, market, stratname, indicator_val, signal, self.amount, result, profit_loss, self.pnl)
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

def start_bot():
    asyncio.run(ProModularBot().run())

def run_dashboard():
    st.title("Pro Ultimate Martingale Deriv Bot Dashboard")
    st.write("---")
    d = dashboard_state
    col1, col2, col3 = st.columns(3)
    col1.metric("Market", d['current_market'])
    col2.metric("Strategy", d['current_strategy'])
    col3.metric("Signal", d['current_signal'])
    st.metric("Stake", d['stake'])
    st.metric("Loss Streak", d['loss_streak'])
    st.metric("Last Indicator", d['indicator_value'])
    st.metric("Last Result", d['result'])
    st.metric("Last Profit", d['profit'])
    st.metric("Cumulative PnL", sum(x['profit'] for x in d['trade_log']) if d['trade_log'] else 0)
    st.write(f"**Status:** {d['bot_status']}")
    st.subheader("Recent Trades")
    st.table(list(d['trade_log']))

if __name__ == '__main__':
    # Run bot in a background thread
    threading.Thread(target=start_bot, daemon=True).start()
    # Start dashboard
    run_dashboard()
