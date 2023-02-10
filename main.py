import hmac
import hashlib
import asyncio
import datetime
import urllib.parse
from collections import OrderedDict
from typing import Callable, Any

import aiohttp
from pydantic import BaseSettings


async def make_any_request(
    url: str, method: str, **kwargs
) -> list[dict[str, Any]] | dict[str, Any]:
    await asyncio.sleep(0.5)

    async with aiohttp.ClientSession() as session:
        method = getattr(session, method)
        async with method(url=url, **kwargs) as request:
            response = await request.json()
            return response


class Settings(BaseSettings):
    api_key: str
    api_secret: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class BinanceFuture:
    def __init__(
        self,
        settings: Settings,
        base_url: str | None = None,
        exchange_info_endpoint: str | None = None,
        user_trades_endpoint: str | None = None,
    ):
        self.settings: Settings = settings
        self.base_url: str = base_url or "https://testnet.binancefuture.com"
        self.exchange_info_endpoint: str = (
            exchange_info_endpoint or "/fapi/v1/exchangeInfo"
        )
        self.user_trades_endpoint: str = user_trades_endpoint or "/fapi/v1/userTrades"

    async def get_symbols(self) -> list[str]:
        url = urllib.parse.urljoin(self.base_url, self.exchange_info_endpoint)
        info = await make_any_request(url=url, method="get")
        trading_symbols = [
            symbol_data["symbol"]
            for symbol_data in info.get("symbols", [])
            if symbol_data.get("status", "").lower() == "trading"
        ]
        return trading_symbols

    async def get_user_trades(
        self,
        symbol: str,
        limit: int = 500,
        from_date: datetime.datetime | None = None,
        to_date: datetime.datetime | None = None,
    ) -> list[str]:
        url = urllib.parse.urljoin(self.base_url, self.user_trades_endpoint)
        params = OrderedDict(
            {
                "symbol": symbol,
                "limit": limit,
            }
        )
        headers = {"X-MBX-APIKEY": self.settings.api_key}

        if limit <= 1 or limit > 1000:
            raise Exception("Limit should be greater than 1 and less or equal to 1000.")

        if not to_date:
            to_date = (
                datetime.datetime.now().replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                if not from_date
                else (from_date + datetime.timedelta(days=7)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            )

        if not from_date:
            from_date = (to_date - datetime.timedelta(days=7)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        if from_date >= to_date:
            raise Exception("from_date cannot be equal or greater than to_date.")
        if (to_date - from_date).days > 7:
            raise Exception(
                "The time between from_date "
                "and to_date cannot be longer than 7 days."
            )

        from_id = None
        trades = []
        batch_latest_trade = None
        while True:
            if from_id:
                params["fromId"] = from_id
                params.pop("startTime", None)
                params.pop("endTime", None)
            else:
                params.update(
                    {
                        "startTime": int(from_date.timestamp()) * 1000,
                        "endTime": int(
                            (from_date + datetime.timedelta(days=1)).timestamp()
                        )
                        * 1000,
                    }
                )
            params["timestamp"] = int(datetime.datetime.now().timestamp()) * 1000
            # binance api requires signature to be the last param
            params.pop("signature", None)
            params["signature"] = self.make_signature(
                message=urllib.parse.urlencode(params).encode("utf-8")
            )

            batch_trades = await make_any_request(
                url, method="get", params=params, headers=headers
            )
            if not batch_trades or batch_trades[-1] == batch_latest_trade:
                break

            trades.extend(
                [
                    t
                    for t in batch_trades
                    if t != batch_latest_trade
                    and datetime.datetime.fromtimestamp(t.get("time") / 1000) < to_date
                ]
            )
            batch_latest_trade = batch_trades[-1]
            if (
                datetime.datetime.fromtimestamp(batch_latest_trade.get("time") / 1000)
                >= to_date
            ):
                break

            from_id = batch_latest_trade["id"]

            await asyncio.sleep(0.5)
        return trades

    def make_signature(
        self,
        message: bytes,
        encoder: Callable = str.encode,
        method: Callable = hashlib.sha256,
    ) -> str:
        return hmac.new(
            encoder(self.settings.api_secret), msg=message, digestmod=method
        ).hexdigest()


async def main():
    settings: Settings = Settings()
    binance: BinanceFuture = BinanceFuture(settings=settings)

    symbols: list[str] = await binance.get_symbols()

    print(symbols)
    await asyncio.sleep(0.1)

    trades = await binance.get_user_trades(
        symbol=symbols[0],
        limit=2,
        from_date=datetime.datetime(year=2023, month=2, day=9),
        to_date=datetime.datetime(year=2023, month=2, day=11),
    )
    print(trades)


if __name__ == "__main__":
    asyncio.run(main())
