import sqlite3, pathlib
for p in [pathlib.Path('market_predictor.db'), pathlib.Path('gamma_analysis.db'), pathlib.Path('market.db'), pathlib.Path('data/market_data.db')]:
    print('\nDB', p, 'exists', p.exists())
    if not p.exists():
        continue
    con = sqlite3.connect(p)
    cur = con.cursor()
    tables = [row[0] for row in cur.execute("select name from sqlite_master where type='table' order by name").fetchall()]
    print('tables', tables)
    for name in tables[:12]:
        try:
            print(name, cur.execute(f'select count(*) from {name}').fetchone()[0])
        except Exception as exc:
            print(name, 'ERR', exc)
    con.close()
