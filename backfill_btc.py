"""
BTC 초기 가격 데이터 백필 스크립트
- Blockchain.com API로 2009~현재 일봉 데이터 다운로드 (무료, API 키 불필요)
- 기존 BTC_USD.csv와 병합 (기존 yfinance 데이터 우선)
- 한 번만 실행하면 됨 (이후엔 기존 update_data.py가 yfinance로 업데이트)

사용법:
  pip install requests pandas
  python backfill_btc.py
"""

import requests
import pandas as pd
import os
from datetime import datetime, timezone

DATA_DIR = "data"
BTC_FILE = os.path.join(DATA_DIR, "BTC_USD.csv")


def fetch_blockchain_com():
    """Blockchain.com API에서 BTC 일별 시장 가격 다운로드 (2009~)"""
    print("📡 Blockchain.com API에서 BTC 전체 히스토리 다운로드 중...")

    url = "https://api.blockchain.info/charts/market-price"
    params = {
        "timespan": "all",      # 전체 기간
        "format": "json",
        "sampled": "false",     # 모든 데이터 포인트
        "cors": "true"
    }

    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for point in data.get("values", []):
        ts = point["x"]  # Unix timestamp
        price = point["y"]  # USD price
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d")

        if price > 0:  # 가격 0인 날 제외
            rows.append({
                "Date": date_str,
                "Close": price,
                "High": price,      # Blockchain.com은 종가만 제공
                "Low": price,
                "Open": price,
                "Volume": 0
            })

    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"])
    # 같은 날짜 중복 제거 (마지막 값 유지)
    df = df.drop_duplicates(subset="Date", keep="last")
    df = df.sort_values("Date").reset_index(drop=True)

    print(f"  → Blockchain.com: {len(df)}행, {df['Date'].min().date()} ~ {df['Date'].max().date()}")
    return df


def load_existing_csv():
    """기존 BTC_USD.csv 로드"""
    if not os.path.exists(BTC_FILE):
        print(f"  ⚠️ {BTC_FILE} 없음, 새로 생성합니다")
        return pd.DataFrame()

    df = pd.read_csv(BTC_FILE)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    print(f"  → 기존 CSV: {len(df)}행, {df['Date'].min().date()} ~ {df['Date'].max().date()}")
    return df


def merge_data(blockchain_df, existing_df):
    """
    병합 전략:
    - 기존 yfinance 데이터가 있는 날짜 → yfinance 우선 (OHLCV 정확)
    - 기존 데이터가 없는 날짜 → Blockchain.com 데이터로 채움
    """
    if existing_df.empty:
        print("  → 기존 데이터 없음, Blockchain.com 데이터만 사용")
        return blockchain_df

    # 기존 데이터의 시작일
    existing_start = existing_df["Date"].min()
    print(f"  → 기존 데이터 시작: {existing_start.date()}")

    # Blockchain.com에서 기존 시작일 이전 데이터만 추출
    early_data = blockchain_df[blockchain_df["Date"] < existing_start].copy()
    print(f"  → 백필 대상 (기존 이전): {len(early_data)}행")

    if early_data.empty:
        print("  → 백필할 데이터 없음")
        return existing_df

    # 병합: 초기 데이터 + 기존 데이터
    merged = pd.concat([early_data, existing_df], ignore_index=True)
    merged = merged.drop_duplicates(subset="Date", keep="last")
    merged = merged.sort_values("Date").reset_index(drop=True)

    print(f"  → 병합 완료: {len(merged)}행, {merged['Date'].min().date()} ~ {merged['Date'].max().date()}")
    return merged


def save_csv(df):
    """CSV 저장 (기존 포맷 유지)"""
    os.makedirs(DATA_DIR, exist_ok=True)

    # Date를 문자열로 변환
    save_df = df.copy()
    save_df["Date"] = save_df["Date"].dt.strftime("%Y-%m-%d")

    save_df.to_csv(BTC_FILE, index=False)
    print(f"✅ 저장 완료: {BTC_FILE} ({len(save_df)}행)")


def main():
    print("=" * 50)
    print("BTC 초기 가격 데이터 백필")
    print("=" * 50)

    # 1. Blockchain.com에서 전체 히스토리 다운로드
    blockchain_df = fetch_blockchain_com()

    # 2. 기존 CSV 로드
    existing_df = load_existing_csv()

    # 3. 병합
    merged_df = merge_data(blockchain_df, existing_df)

    # 4. 저장
    save_csv(merged_df)

    # 5. 결과 요약
    print("\n📊 결과 요약:")
    print(f"  기간: {merged_df['Date'].min().date()} ~ {merged_df['Date'].max().date()}")
    print(f"  총 행: {len(merged_df):,}")

    # 연도별 데이터 수
    merged_df["Year"] = merged_df["Date"].dt.year
    yearly = merged_df.groupby("Year").size()
    print("\n  연도별 데이터:")
    for year, count in yearly.items():
        marker = " ← 백필" if year < 2014 else ""
        print(f"    {year}: {count}일{marker}")


if __name__ == "__main__":
    main()
