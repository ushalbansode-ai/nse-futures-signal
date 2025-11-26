import click
from src.fetch_bhavcopy import fetch_dat
from src.parse_dat_bhav import parse_dat_bytes
from src.signals_advanced import generate_signals_advanced

@click.command()
@click.option("--top", default=10, help="Show top N signals")
def run(top):
    raw = fetch_dat()
    df = parse_dat_bytes(raw)
    fut = df[df["INSTRUMENT"].str.contains("FUT")]

    signals = generate_signals_advanced(fut)[:top]
    for s in signals:
        print(s)

if __name__ == "__main__":
    run()
  
