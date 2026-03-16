from pathlib import Path
from datetime import datetime, UTC
import re
import akshare as ak


def sql_escape(value: str) -> str:
    return value.replace("'", "''")


def fetch_a_shares():
    df = ak.stock_info_a_code_name()
    items = []
    for _, row in df.iterrows():
        code = str(row["code"]).strip()
        name = str(row["name"]).strip()
        if not re.fullmatch(r"\d{6}", code):
            continue
        if code.startswith("6"):
            ticker = f"{code}.SS"
            market = "SS"
        elif code.startswith("0") or code.startswith("3"):
            ticker = f"{code}.SZ"
            market = "SZ"
        else:
            continue
        items.append((ticker, name, market, '', ''))
    return items


def fetch_hk_shares():
    df = None
    code_col = None
    name_col = None
    try:
        df = ak.stock_hk_spot_em()
        code_col = "代码" if "代码" in df.columns else df.columns[1]
        name_col = "名称" if "名称" in df.columns else df.columns[2]
    except Exception:
        df = ak.stock_hk_spot()
        code_col = "代码" if "代码" in df.columns else df.columns[1]
        name_col = "中文名称" if "中文名称" in df.columns else df.columns[2]
    items = []
    for _, row in df.iterrows():
        raw_code = str(row[code_col]).strip()
        name = str(row[name_col]).strip()
        code = "".join(ch for ch in raw_code if ch.isdigit())
        if not code:
            continue
        code = code.zfill(5)
        ticker = f"{code}.HK"
        items.append((ticker, name, "HK", '', ''))
    return items


def build_sql(records):
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    lines.append("CREATE DATABASE IF NOT EXISTS mystock CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    lines.append("USE mystock;")
    lines.append("")
    lines.append("CREATE TABLE IF NOT EXISTS stocks (")
    lines.append("  ticker VARCHAR(20) PRIMARY KEY,")
    lines.append("  name VARCHAR(100) NULL,")
    lines.append("  market VARCHAR(10) NULL,")
    lines.append("  sector VARCHAR(100) NULL,")
    lines.append("  industry VARCHAR(100) NULL,")
    lines.append("  description TEXT NULL,")
    lines.append("  last_updated DATETIME NULL")
    lines.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
    lines.append("ALTER TABLE stocks CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    lines.append("")
    lines.append("CREATE TABLE IF NOT EXISTS company_seed (")
    lines.append("  ticker VARCHAR(20) PRIMARY KEY,")
    lines.append("  name VARCHAR(100) NOT NULL,")
    lines.append("  market VARCHAR(10) NOT NULL,")
    lines.append("  industry VARCHAR(100) NULL,")
    lines.append("  sector VARCHAR(100) NULL")
    lines.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
    lines.append("ALTER TABLE company_seed CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    lines.append("")
    lines.append("TRUNCATE TABLE company_seed;")
    lines.append("")
    batch = 1000
    for i in range(0, len(records), batch):
        chunk = records[i : i + batch]
        values = []
        for ticker, name, market in chunk:
            values.append(
                f"('{sql_escape(ticker)}','{sql_escape(name)}','{sql_escape(market)}')"
            )
        lines.append("INSERT INTO company_seed (ticker, name, market) VALUES")
        lines.append(",\n".join(values) + ";")
        lines.append("")
    lines.append(
        "INSERT INTO stocks (ticker, name, market, sector, industry, description, last_updated)"
    )
    lines.append(
        "SELECT s.ticker, s.name, s.market, 'Unknown', 'Unknown', '', "
        f"'{now}' FROM company_seed s WHERE NOT EXISTS (SELECT 1 FROM stocks LIMIT 1);"
    )
    lines.append("")
    lines.append("SELECT COUNT(*) AS imported_company_count FROM company_seed;")
    lines.append(
        "SELECT COUNT(*) AS stocks_count_after_insert FROM stocks;"
    )
    return "\n".join(lines) + "\n"


def main():
    records = {}
    for ticker, name, market, industry, sector in fetch_a_shares():
        records[ticker] = (ticker, name, market, industry, sector)
    for ticker, name, market, industry, sector in fetch_hk_shares():
        records[ticker] = (ticker, name, market, industry, sector)
    sorted_records = sorted(records.values(), key=lambda x: (x[2], x[0]))
    out_dir = Path("sql")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "import_all_companies.sql"
    out_file.write_text(build_sql(sorted_records), encoding="utf-8")
    print(f"generated: {out_file} records={len(sorted_records)}")


if __name__ == "__main__":
    main()
