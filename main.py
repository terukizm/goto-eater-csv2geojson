import os
import logging
import logzero
from distutils.util import strtobool
import argparse
import pathlib
from logzero import logger
from csv2geojson.parser import Csv2GeoJSON

def main(input_dir, output_dir, pref_list: list):
    # 雑にログ出力設定
    # MEMO: 開発時はdocker-compose.ymlからEnvでLOGGER_DEBUG=Trueを指定
    # 運用時は_error.jsonを見ろって感じなのでDebug出力なし
    logzero.loglevel(logging.WARNING)
    if strtobool(os.getenv('LOGGER_DEBUG', 'False')):
        logzero.loglevel(logging.DEBUG)

    # MEMO: 並列処理してあげると多少早く終わるかも
    for pref in sorted(pref_list):
        try:
            src = input_dir / f'{pref}.csv'
            parser = Csv2GeoJSON(src)
            parser.write_all(output_dir / pref)
        except Exception as e:
            logger.error(f'[{pref}] ERROR.')
            logger.error(e, stack_info=True)


if __name__ == "__main__":
    # usage:
    # $ docker-compose run csv2geojson python main.py --target tochigi

    # TODO: もう少し真面目にしてあげたい
    parser = argparse.ArgumentParser(description='goto-eat-csv2geojson')
    parser.add_argument('--target', help='例: tochigi')   # 個別指定オプション
    args = parser.parse_args()

    input_dir = pathlib.Path(__file__).parent / 'data' / 'input' / 'csvs'
    output_dir = pathlib.Path(__file__).parent / 'data' / 'output'

    # --target 指定がなければ data/input/csvs/ 以下の *.csv 全てを対象
    pref_list = args.target.split(',') if args.target else [x.stem for x in input_dir.glob('*.csv')]

    print(f'pref_list = {pref_list}')
    main(input_dir, output_dir, pref_list)
    print(f'done.')

