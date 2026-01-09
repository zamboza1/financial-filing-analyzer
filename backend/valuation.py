"""Stock valuation metrics and market data."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import re


@dataclass
class MarketData:
    """
    Market data for a stock (current or historical).
    
    All monetary values are in actual dollars unless noted.
    Market cap is in millions USD for consistency with KPI data.
    """
    ticker: str
    current_price: Optional[float] = None  # Stock price ($)
    market_cap: Optional[float] = None  # Market cap (millions USD)
    shares_outstanding: Optional[float] = None  # Shares (millions)
    fifty_two_week_high: Optional[float] = None  # 52-week high ($)
    fifty_two_week_low: Optional[float] = None  # 52-week low ($)
    average_volume: Optional[float] = None  # Avg daily volume
    beta: Optional[float] = None  # Beta coefficient
    dividend_yield: Optional[float] = None  # Dividend yield (decimal)
    is_historical: bool = False  # True if using historical price
    price_date: Optional[str] = None  # Date of the price (for historical)


@dataclass
class ValuationRatios:
    """
    Calculated valuation ratios.
    
    Combines market data with financial metrics from SEC filings.
    """
    # Valuation multiples
    pe_ratio: Optional[float] = None  # Price / EPS
    forward_pe: Optional[float] = None  # Price / Forward EPS (if available)
    ps_ratio: Optional[float] = None  # Market Cap / Revenue (annualized)
    pb_ratio: Optional[float] = None  # Market Cap / Book Value
    peg_ratio: Optional[float] = None  # PE / Growth Rate
    
    # Enterprise value ratios
    ev_to_ebitda: Optional[float] = None  # EV / EBITDA
    ev_to_revenue: Optional[float] = None  # EV / Revenue
    
    # Efficiency ratios
    revenue_per_share: Optional[float] = None  # Revenue / Shares
    book_value_per_share: Optional[float] = None  # Book Value / Shares
    
    # Market metrics
    market_cap: Optional[float] = None  # In millions USD
    enterprise_value: Optional[float] = None  # In millions USD


def fetch_historical_price(ticker: str, target_date: str) -> tuple[Optional[float], Optional[str]]:
    """
    Fetch historical closing price for a specific date.
    
    Args:
        ticker: Stock ticker symbol
        target_date: Date string in format "YYYY-MM-DD"
        
    Returns:
        Tuple of (price, actual_date) - actual_date may differ if market was closed
    """
    try:
        import yfinance as yf
        
        # Parse the target date
        target = datetime.strptime(target_date, "%Y-%m-%d")
        
        # Get a range of dates around the target (in case of weekends/holidays)
        start = target - timedelta(days=7)
        end = target + timedelta(days=3)
        
        stock = yf.Ticker(ticker)
        history = stock.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
        
        if history.empty:
            return None, None
        
        # Find the closest date on or before the target
        history.index = history.index.tz_localize(None)  # Remove timezone for comparison
        valid_dates = history.index[history.index <= target]
        
        if len(valid_dates) == 0:
            # No dates on or before target, use the first available
            actual_date = history.index[0]
        else:
            # Use the closest date on or before target
            actual_date = valid_dates[-1]
        
        price = float(history.loc[actual_date, 'Close'])
        date_str = actual_date.strftime("%Y-%m-%d")
        
        return price, date_str
        
    except Exception as e:
        print(f"     ⚠️ Could not fetch historical price: {e}")
        return None, None


def fetch_historical_52week_range(ticker: str, target_date: str) -> tuple[Optional[float], Optional[float]]:
    """
    Calculate 52-week high and low as of a specific date.
    
    Args:
        ticker: Stock ticker symbol
        target_date: Date string in format "YYYY-MM-DD"
        
    Returns:
        Tuple of (52_week_low, 52_week_high)
    """
    try:
        import yfinance as yf
        
        target = datetime.strptime(target_date, "%Y-%m-%d")
        
        # Get 52 weeks (+ buffer) of data ending at target date
        start = target - timedelta(weeks=53)
        end = target + timedelta(days=1)
        
        stock = yf.Ticker(ticker)
        history = stock.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
        
        if history.empty or len(history) < 10:
            return None, None
        
        # Remove timezone and filter to dates <= target
        history.index = history.index.tz_localize(None)
        history = history[history.index <= target]
        
        # Get last 52 weeks of data
        one_year_ago = target - timedelta(weeks=52)
        history_52w = history[history.index >= one_year_ago]
        
        if len(history_52w) < 10:
            return None, None
        
        low = float(history_52w['Low'].min())
        high = float(history_52w['High'].max())
        
        print(f"     📊 Historical 52-week range: ${low:.2f} - ${high:.2f}")
        return low, high
        
    except Exception as e:
        print(f"     ⚠️ Could not fetch 52-week range: {e}")
        return None, None


def fetch_historical_dividend_yield(ticker: str, target_date: str, price: float) -> Optional[float]:
    """
    Calculate trailing 12-month dividend yield as of a specific date.
    
    Args:
        ticker: Stock ticker symbol
        target_date: Date string in format "YYYY-MM-DD"
        price: Stock price on the target date
        
    Returns:
        Dividend yield as decimal (e.g., 0.025 for 2.5%)
    """
    try:
        import yfinance as yf
        
        if not price or price <= 0:
            return None
        
        target = datetime.strptime(target_date, "%Y-%m-%d")
        
        # Get dividends for trailing 12 months
        start = target - timedelta(days=400)  # Extra buffer
        end = target + timedelta(days=1)
        
        stock = yf.Ticker(ticker)
        dividends = stock.dividends
        
        if dividends.empty:
            return None
        
        # Filter to trailing 12 months
        dividends.index = dividends.index.tz_localize(None)
        one_year_ago = target - timedelta(days=365)
        trailing_divs = dividends[(dividends.index >= one_year_ago) & (dividends.index <= target)]
        
        if trailing_divs.empty:
            return None
        
        annual_dividend = float(trailing_divs.sum())
        dividend_yield = annual_dividend / price
        
        print(f"     📊 Historical dividend yield: {dividend_yield*100:.2f}% (${annual_dividend:.2f}/share)")
        return dividend_yield
        
    except Exception as e:
        print(f"     ⚠️ Could not fetch dividend yield: {e}")
        return None


def fetch_market_data(
    ticker: str, 
    period_end: Optional[str] = None,
    filing_shares: Optional[float] = None
) -> MarketData:
    """
    Fetch market data for a stock using yfinance.
    
    If period_end is provided and is more than 30 days old, fetches historical
    price from that date. Otherwise uses current price.
    
    Args:
        ticker: Stock ticker symbol
        period_end: Optional period end date (YYYY-MM-DD) for historical lookup
        filing_shares: Optional shares outstanding from SEC filing (in millions)
                       Used for more accurate historical market cap
        
    Returns:
        MarketData with price, market cap, etc.
    """
    try:
        import yfinance as yf
        
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get shares outstanding - prefer filing data for historical accuracy
        current_shares = _to_millions(info.get('sharesOutstanding'))
        shares_outstanding = filing_shares if filing_shares else current_shares
        
        # Determine if we should use historical price
        use_historical = False
        historical_price = None
        price_date = None
        
        if period_end:
            try:
                period_date = datetime.strptime(period_end, "%Y-%m-%d")
                days_ago = (datetime.now() - period_date).days
                
                # Use historical price if period is more than 30 days old
                if days_ago > 30:
                    historical_price, price_date = fetch_historical_price(ticker, period_end)
                    if historical_price:
                        use_historical = True
                        print(f"     📈 Historical price ({price_date}): ${historical_price:.2f}")
            except ValueError:
                pass  # Invalid date format, use current
        
        if use_historical and historical_price:
            # Calculate historical market cap using filing shares if available
            hist_market_cap = historical_price * shares_outstanding if shares_outstanding else None
            
            shares_source = "filing" if filing_shares else "current"
            if hist_market_cap:
                print(f"     📊 Historical MCap: ${hist_market_cap:,.0f}M ({shares_source} shares)")
            
            # Fetch historical 52-week range and dividend yield
            hist_52w_low, hist_52w_high = fetch_historical_52week_range(ticker, price_date)
            hist_div_yield = fetch_historical_dividend_yield(ticker, price_date, historical_price)
            
            market_data = MarketData(
                ticker=ticker,
                current_price=historical_price,
                market_cap=hist_market_cap,
                shares_outstanding=shares_outstanding,
                fifty_two_week_high=hist_52w_high,
                fifty_two_week_low=hist_52w_low,
                average_volume=None,  # Could calculate but less useful historically
                beta=info.get('beta'),  # Beta is relatively stable
                dividend_yield=hist_div_yield,
                is_historical=True,
                price_date=price_date
            )
        else:
            # Use current market data
            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            market_data = MarketData(
                ticker=ticker,
                current_price=current_price,
                market_cap=_to_millions(info.get('marketCap')),
                shares_outstanding=shares_outstanding,
                fifty_two_week_high=info.get('fiftyTwoWeekHigh'),
                fifty_two_week_low=info.get('fiftyTwoWeekLow'),
                average_volume=info.get('averageVolume'),
                beta=info.get('beta'),
                dividend_yield=info.get('dividendYield'),
                is_historical=False,
                price_date=datetime.now().strftime("%Y-%m-%d")
            )
            if current_price:
                print(f"     📈 Current price: ${current_price:.2f}, MCap ${market_data.market_cap:,.0f}M")
        
        return market_data
        
    except ImportError:
        print("     ⚠️ yfinance not installed. Run: pip install yfinance")
        return MarketData(ticker=ticker)
    except Exception as e:
        print(f"     ⚠️ Could not fetch market data: {e}")
        return MarketData(ticker=ticker)


def calculate_valuation_ratios(
    market_data: MarketData,
    eps: Optional[float] = None,
    revenue: Optional[float] = None,
    ebitda: Optional[float] = None,
    net_income: Optional[float] = None,
    total_debt: Optional[float] = None,
    cash: Optional[float] = None,
    book_value: Optional[float] = None,
    is_quarterly: bool = True
) -> ValuationRatios:
    """
    Calculate valuation ratios from market data and financial metrics.
    
    Args:
        market_data: Current market data (price, market cap, etc.)
        eps: Earnings per share (quarterly or annual)
        revenue: Revenue in millions (quarterly or annual)
        ebitda: EBITDA in millions (quarterly or annual)
        net_income: Net income in millions
        total_debt: Total debt in millions
        cash: Cash and equivalents in millions
        book_value: Total shareholders' equity in millions
        is_quarterly: If True, annualize quarterly figures for ratios
        
    Returns:
        ValuationRatios with calculated metrics
    """
    ratios = ValuationRatios()
    
    price = market_data.current_price
    market_cap = market_data.market_cap
    shares = market_data.shares_outstanding
    
    if not price or not market_cap:
        return ratios
    
    ratios.market_cap = market_cap
    
    # Annualization factor (multiply quarterly by 4)
    ann_factor = 4 if is_quarterly else 1
    
    # P/E Ratio
    if eps and eps > 0:
        annual_eps = eps * ann_factor
        ratios.pe_ratio = price / annual_eps
        print(f"     📊 P/E Ratio: {ratios.pe_ratio:.1f}x (Price ${price:.2f} / Annual EPS ${annual_eps:.2f})")
    
    # P/S Ratio (Price to Sales)
    if revenue and revenue > 0 and market_cap:
        annual_revenue = revenue * ann_factor
        ratios.ps_ratio = market_cap / annual_revenue
        ratios.revenue_per_share = annual_revenue / shares if shares else None
        print(f"     📊 P/S Ratio: {ratios.ps_ratio:.2f}x")
    
    # Enterprise Value
    if market_cap:
        debt = total_debt or 0
        cash_val = cash or 0
        ratios.enterprise_value = market_cap + debt - cash_val
        
        # EV/EBITDA
        if ebitda and ebitda > 0:
            annual_ebitda = ebitda * ann_factor
            ratios.ev_to_ebitda = ratios.enterprise_value / annual_ebitda
            print(f"     📊 EV/EBITDA: {ratios.ev_to_ebitda:.1f}x")
        
        # EV/Revenue
        if revenue and revenue > 0:
            annual_revenue = revenue * ann_factor
            ratios.ev_to_revenue = ratios.enterprise_value / annual_revenue
    
    # P/B Ratio (Price to Book)
    if book_value and book_value > 0 and market_cap:
        ratios.pb_ratio = market_cap / book_value
        ratios.book_value_per_share = book_value / shares if shares else None
        print(f"     📊 P/B Ratio: {ratios.pb_ratio:.2f}x")
    
    return ratios


def extract_shares_outstanding(text: str) -> Optional[float]:
    """
    Extract shares outstanding from SEC filing text.
    
    SEC filings typically report shares in millions or actual count.
    Returns shares in millions.
    """
    patterns = [
        # "15,115,823 thousand shares" or similar
        r'([\d,]+)\s*(?:thousand|000)\s*shares?\s+(?:outstanding|issued)',
        # "shares outstanding: 15,115,823,000"
        r'shares?\s+outstanding[:\s]*([\d,]+)',
        # Common stock outstanding
        r'common\s+stock[^0-9]*([\d,]+)\s*(?:shares?|thousand)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                value_str = match.group(1).replace(',', '')
                value = float(value_str)
                
                # Convert to millions
                if value > 1_000_000_000:  # Billions (actual shares)
                    return value / 1_000_000
                elif value > 1_000_000:  # Millions
                    return value / 1_000_000
                elif value > 1_000:  # Thousands
                    return value / 1_000
                else:
                    return value  # Already in millions
            except ValueError:
                continue
    
    return None


def _to_millions(value: Optional[float]) -> Optional[float]:
    """Convert a value to millions."""
    if value is None:
        return None
    return value / 1_000_000

