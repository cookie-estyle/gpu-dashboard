import argparse
import datetime as dt
import os
import sys
import yaml

from src.tracker.run_manager import RunManager
from src.uploader.run_uploader import RunUploader
from src.blacklist.blacklist_manager import update_blacklist
from src.utils.config import CONFIG

def validate_dates(start_date, end_date):
    if (start_date is None and end_date is None) or (start_date is not None and end_date is not None):
        if start_date is None and end_date is None:
            # 両方指定なしの場合、昨日から今日までの範囲を設定
            today = dt.date.today()
            yesterday = today - dt.timedelta(days=1)
            return yesterday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
        else:
            # 両方指定ありの場合、日付の妥当性をチェック
            try:
                start = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
                end = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
                if start > end:
                    raise ValueError("Start date must be before or equal to end date.")
                return start_date, end_date
            except ValueError as e:
                print(f"Error: Invalid date format or range. {e}")
                sys.exit(1)
    else:
        print("Error: Both start-date and end-date must be provided, or neither should be provided.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Fetch and process run data from Weights & Biases")
    parser.add_argument("--api", type=str, help="Weights & Biases API Key")
    parser.add_argument("--start-date", type=str, help="Start date for data fetch (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date for data fetch (YYYY-MM-DD)")
    args = parser.parse_args()

    # API キーの処理
    if args.api is not None:
        if "WANDB_API_KEY" in os.environ:
            del os.environ["WANDB_API_KEY"]
        os.environ["WANDB_API_KEY"] = args.api
    elif "WANDB_API_KEY" not in os.environ:
        print("Warning: Weights & Biases API Key not provided. Some features may not work.")

    # 日付の検証
    start_date, end_date = validate_dates(args.start_date, args.end_date)
    date_range = [start_date, end_date]

    # 他の環境変数の設定
    os.environ["WANDB_CACHE_DIR"] = CONFIG.get('wandb_dir', '/tmp/wandb')
    os.environ["WANDB_DATA_DIR"] = CONFIG.get('wandb_dir', '/tmp/wandb')
    os.environ["WANDB_DIR"] = CONFIG.get('wandb_dir', '/tmp/wandb')

    print(f"Fetching data from {start_date} to {end_date}")

    # RunManagerの初期化と実行
    run_manager = RunManager(date_range)
    new_runs_df = run_manager.fetch_runs()

    # ブラックリストの更新
    # print("Updating blacklist...")
    # update_blacklist(new_runs_df)
    # print("Blacklist update completed.")

    # RunUploaderを使用してデータを処理しアップロード
    uploader = RunUploader(new_runs_df, date_range)
    processed_df = uploader.process_and_upload_runs()

    # CSVファイルに保存
    processed_df.write_csv("combined_runs_data.csv")
    print("Data processing and uploading completed successfully.")

if __name__ == "__main__":
    main()