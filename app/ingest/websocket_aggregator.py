"""
WebSocket ingestion with 1-second aggregation to prevent CPU overload.
Normalizes all payloads to IndexTick and OptTrade at ingest boundary.
"""
import asyncio
import logging
import time
from collections import defaultdict
from typing import Dict, List
from datetime import datetime, date

from app.state.ring_buffers import PREDICTION_SYMBOLS, TRACKED_INDEX_SYMBOLS

log = logging.getLogger("ws_ingest")

from app.state.ring_buffers import PREDICTION_SYMBOLS, TRACKED_INDEX_SYMBOLS

# Sub-second accumulator per symbol
_accum: Dict[str, Dict[int, List]] = defaultdict(lambda: defaultdict(list))

def parse_index_value(msg: dict) -> tuple:
    """
    Parse Polygon Value message for indices.
    Value messages provide real-time index value updates.
    Returns (symbol, IndexTick) or None if invalid.
    
    Format: {ev: "V", val: 3988.5, T: "I:SPX", t: 1678220098130}
    """
    from app.state.ring_buffers import IndexTick
    
    try:
        # Get ticker with I: prefix
        ticker = msg.get("T", "")
        
        # Extract symbol without I: prefix
        if ticker.startswith("I:"):
            symbol = ticker[2:]
        else:
            return None
        
        if symbol not in TRACKED_INDEX_SYMBOLS:
            return None
        
        # Get value and timestamp
        value = msg.get("val")  # Index value
        ts_ms = msg.get("t")   # Timestamp in milliseconds
        
        if value is None or ts_ms is None:
            return None
        
        # Convert to seconds
        ts = ts_ms // 1000
        
        # Volume is N/A for value updates, use 1.0
        size = 1.0
        
        tick = IndexTick(ts=ts, price=float(value), size=size)
        
        return (symbol, tick)
        
    except Exception as e:
        log.warning(f"Failed to parse index value: {e}")
        return None

def parse_index_aggregate(msg: dict) -> tuple:
    """
    Parse Polygon aggregate message for indices.
    Returns (symbol, IndexTick) or None if invalid.
    """
    from app.state.ring_buffers import IndexTick
    
    try:
        # Polygon sends: ev=A, sym=I:SPX, c=close, v=volume (None for indices), s=start_ts
        sym = msg.get("sym", "")
        
        # Extract symbol without I: prefix
        if sym.startswith("I:"):
            symbol = sym[2:]
        else:
            return None
        
        if symbol not in TRACKED_INDEX_SYMBOLS:
            return None
        
        # Get price and timestamp
        price = msg.get("c")  # Close price of this bar
        ts_ms = msg.get("s")  # Start timestamp (milliseconds)
        
        if price is None or ts_ms is None:
            return None
        
        # Convert to seconds
        ts = ts_ms // 1000
        
        # Volume is None for indices, use 1.0
        size = 1.0
        
        tick = IndexTick(ts=ts, price=float(price), size=size)
        
        return (symbol, tick)
        
    except Exception as e:
        log.warning(f"Failed to parse index aggregate: {e}")
        return None

def parse_options_trade(msg: dict) -> tuple:
    """
    Parse Polygon options trade message.
    Returns (root_symbol, OptTrade) or None if invalid.
    """
    from app.state.ring_buffers import OptTrade
    
    try:
        # Polygon sends: ev=T, sym=O:SPX241108C06000000, p=price, s=size, ...
        sym = msg.get("sym", "")
        
        if not sym.startswith("O:"):
            return None
        
        # Parse OCC symbol: O:SPX241108C06000000
        # Format: O:{root}{yymmdd}{C|P}{strike*1000}
        parts = sym[2:]  # Remove O:
        
        # Extract root (SPX, SPXW, etc)
        root = None
        for r in PREDICTION_SYMBOLS:
            if parts.startswith(r):
                root = r
                break
        
        if not root:
            return None
        
        # Extract expiration (next 6 chars after root)
        exp_str = parts[len(root):len(root)+6]
        exp_date = datetime.strptime(f"20{exp_str}", "%Y%m%d").date()
        
        # Extract call/put flag
        cp_flag = parts[len(root)+6]
        is_call = (cp_flag == "C")
        
        # Extract strike (remaining digits / 1000)
        strike_str = parts[len(root)+7:]
        strike = int(strike_str) // 1000
        
        # Get trade details
        price = msg.get("p")
        size = msg.get("s", 1)
        ts_ms = msg.get("t")
        
        if price is None or ts_ms is None:
            return None
        
        ts = ts_ms // 1000
        notional = price * size * 100  # Options contract multiplier
        
        # Determine aggressor (simplified - use exchange code if available)
        aggressor = 1  # Default to buy side
        
        trade = OptTrade(
            ts=ts,
            root=root,
            K=strike,
            is_call=is_call,
            exp=exp_date,
            notional=notional,
            aggressor=aggressor
        )
        
        return (root, trade)
        
    except Exception as e:
        log.warning(f"Failed to parse options trade: {e}")
        return None

def aggregate_ticks(symbol: str, ticks: List) -> tuple:
    """
    Aggregate sub-second ticks into 1-second bar.
    Returns (timestamp, IndexTick with aggregated data).
    """
    from app.state.ring_buffers import IndexTick
    
    if not ticks:
        return None
    
    # Use latest timestamp
    ts = ticks[-1].ts
    
    # Use VWAP for aggregated price
    total_pv = sum(t.price * t.size for t in ticks)
    total_v = sum(t.size for t in ticks)
    
    if total_v > 0:
        agg_price = total_pv / total_v
    else:
        agg_price = ticks[-1].price
    
    return (ts, IndexTick(ts=ts, price=agg_price, size=total_v))

async def ingest_message(msg: dict):
    """
    Ingest a single WebSocket message.
    Routes to appropriate parser and accumulates for 1-second aggregation.
    """
    ev = msg.get("ev")
    
    if ev == "V":  # Value update (primary index feed)
        result = parse_index_value(msg)
        if result:
            symbol, tick = result
            
            # Accumulate by second
            ts_second = tick.ts
            _accum[symbol][ts_second].append(tick)
    
    elif ev == "A":  # Aggregate (index bars) - fallback
        result = parse_index_aggregate(msg)
        if result:
            symbol, tick = result
            
            # Accumulate by second
            ts_second = tick.ts
            _accum[symbol][ts_second].append(tick)
    
    elif ev == "T":  # Trade (options)
        result = parse_options_trade(msg)
        if result:
            root, trade = result
            
            # Add directly to flow ring (trades already atomic)
            from app.state.ring_buffers import FLOW_RINGS
            FLOW_RINGS[root].add(trade.ts, trade)

async def flush_aggregates():
    """
    Periodically flush accumulated ticks to ring buffers.
    Runs every 1 second to create 1-second bars.
    Also updates ORB tracker with each price tick.
    """
    from app.state.ring_buffers import INDEX_RINGS, update_session_vwap
    from app.state.orb_tracker import update_orb
    
    while True:
        try:
            await asyncio.sleep(1.0)
            
            current_second = int(time.time())
            
            for symbol in list(_accum.keys()):
                symbol_accum = _accum[symbol]
                
                # Flush all complete seconds (not current second)
                for ts_second in list(symbol_accum.keys()):
                    if ts_second < current_second:
                        ticks = symbol_accum.pop(ts_second)
                        
                        if ticks:
                            result = aggregate_ticks(symbol, ticks)
                            if result:
                                ts, agg_tick = result
                                
                                # Add to ring buffer
                                INDEX_RINGS[symbol].add(ts, agg_tick)
                                
                                # Update VWAP tracker
                                update_session_vwap(symbol, agg_tick.price, agg_tick.size)
                                
                                # Update ORB tracker (tracks high/low during 9:30-10:30 AM ET)
                                update_orb(symbol, agg_tick.price)
        
        except Exception as e:
            log.error(f"Error in flush_aggregates: {e}")
