from __future__ import annotations

import os
from datetime import date

import databento as db
from dotenv import load_dotenv

ROOTS = [
    "SPX", "SPXW", "XSP",
    "NDX", "NDXP", "XND",
    "RUT", "RUTW", "MRUT",
    "VIX", "VIXW",
    "OEX", "DJX",
    "MID", "SML", "COMP", "NYA", "RUI", "RUA",
    "SOX", "XAU", "HGX", "OSX", "UTY",
]


def main() -> None:
    load_dotenv()
    client = db.Historical(os.environ["DATABENTO_API_KEY"])
    end = client.metadata.get_dataset_range(dataset="OPRA.PILLAR")["schema"]["definition"]["end"]
    print(f"OPRA.PILLAR definition end: {end}")
    for root in ROOTS:
        parent = f"{root}.OPT"
        try:
            data = client.timeseries.get_range(
                dataset="OPRA.PILLAR",
                schema="definition",
                symbols=parent,
                stype_in="parent",
                start=date.today().isoformat(),
                end=end,
                limit=1,
            )
            df = data.to_df()
            if df.empty:
                print(f"{root:5} NO_ROWS")
            else:
                row = df.iloc[0]
                print(
                    f"{root:5} OK      "
                    f"sample={row.get('symbol')} "
                    f"underlying={row.get('underlying')} "
                    f"exp={row.get('expiration')} "
                    f"strike={row.get('strike_price')}"
                )
        except Exception as exc:
            message = str(exc).splitlines()[0]
            print(f"{root:5} ERR     {message[:140]}")


if __name__ == "__main__":
    main()
