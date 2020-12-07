import argparse
import csv
import json
import pathlib
import shutil
from collections import OrderedDict
from urllib.parse import quote

import pandas as pd
import numpy as np
import logging
import logzero
from logzero import logger
from pydams import DAMS
from geojson import Feature, FeatureCollection, Point

from util import geocode, normalize, NormalizeError, GeocodeError
import genre

print('INIT PYDAMS')
DAMS.init_dams()

# @see goto_eat_scrapy.settings.FEED_EXPORT_FIELDS
FEED_EXPORT_FIELDS = ['shop_name', 'address', 'tel', 'genre_name', 'zip_code', 'official_page', 'opening_hours', 'closing_day', 'area_name', 'detail_page']

NORMALIZED_CSV_EXPORT_FIELDS = FEED_EXPORT_FIELDS + [
    'lat',
    'lng',
    'normalized_address',           # ジオコーディングを通すために正規化された住所
    'genre_code',                   # genre.classify()されたジャンルコード(1〜10)
    '_ジオコーディングの結果スコア',
    '_ジオコーディングで無視された住所情報(tail)',
    '_ジオコーディング結果に紐づく住所情報(name)'
]

def _make_feature(row: pd.Series, debug=True):
    """
    GeoJSONのFeature要素(POINT限定)を作成
    @see https://pypi.org/project/geojson/#point
    """
    try:
        # debugオプションの有無でprefixに'_'がついた項目を出し分け
        properties = OrderedDict(row) if debug \
            else OrderedDict({k: v for k, v in OrderedDict(row).items() if not k.startswith('_') })

        # lat, lngは「properites」ではなく「geometry」に配置
        lat = properties.pop('lat')
        lng = properties.pop('lng')

        # その他の補足情報を追加
        googlemap_q_string = '{} {}'.format(properties['normalized_address'], properties['shop_name'])
        properties['GoogleMap'] = 'https://www.google.com/maps/search/?q=' + quote(googlemap_q_string)
        if debug:
            properties['_国土地理院地図のURL'] = f'https://maps.gsi.go.jp/#17/{lat}/{lng}/'
    except Exception as e:
        logger.error(properties)
        raise e

    coords = (lng, lat) # lng, latの順番に注意
    return Feature(geometry=Point(coords), properties=properties)


def _normalize(row: pd.Series):
    """
    ・カテゴリ名
    ・住所名
    """
    try:
        # ジャンル分け
        genre_code = genre.classify(row['genre_name'])
        row['genre_code'] = int(genre_code)

        # 住所の正規化、ジオコーディング
        address = row['address']
        normalized_address = normalize(address)
        lat, lng, _debug = geocode(address)

        row['lat'] = lat
        row['lng'] = lng
        row['normalized_address'] = normalized_address
        row['_ジオコーディングの結果スコア'] = _debug[0]
        row['_ジオコーディング結果に紐づく住所情報(name)'] = _debug[1]
        row['_ジオコーディングで無視された住所情報(tail)'] = _debug[2]
        row['_ERROR'] = np.nan

    except (NormalizeError, GeocodeError) as e:
        # 住所の正規化エラー(NormalizeError), ジオコーディングエラー(GeocodeError)が発生した場合
        # dfの_ERROR列に発生したエラー名を追加、後から追えるようにしておく
        name = e.__class__.__name__
        row['_ERROR'] = name
        logger.warning('{}: {}'.format(name, row.to_dict()))
    except Exception as e:
        # それ以外の例外が発生した場合にはコケさせる
        logger.error('{}: {}'.format(name, row.to_dict()))
        raise e

    return row


# def main(base: str, cleanup=True):
#     # CSV読み込み
#     logger.info(f'base={base}')
#     infile = pathlib.Path.cwd() / f'../data/csv/{base}.csv'

#     # CSVの全カラムを文字列として読み込む
#     # MEMO: データ元のサイト依存だが、書式によってはtel, zip_codeなどがintと認識されるため
#     # 基本的にクローラー側では書式変換はせず、(必要であれば)本csv2geojson側で書式変換を行う方針
#     df = pd.read_csv(infile, encoding="utf-8", dtype=str).fillna('')

#     # 読み込んだCSVデータの重複レコードチェック(店名 and 住所)
#     # 入力データチェックとして十分とは言えないが、参考程度に
#     # MEMO: 以下のパターンがあるので、店名および住所単体だと重複判定できない。
#     # ・同じ店名の別の店(例: 愛知県の「すずや」)
#     # ・同じ住所の別の店(例: 同じショッピングモール内)
#     duplicated_records = df[df.duplicated(subset=['shop_name', 'address'])]
#     if not duplicated_records.empty:
#         logger.warning('以下のレコードが重複しています')
#         logger.warning(duplicated_records)
#         # 重複行の削除
#         df.drop_duplicates(subset=['shop_name', 'address'], keep='last', inplace=True)

#     # normalized_csvの作成
#     # ・カテゴリ名
#     # ・住所
#     # 上記を正規化し、さらにデバッグ用の情報(主にlatlng関係)を付与したもの
#     outfile = pathlib.Path.cwd() / f'../data/normalized_csv/{base}.csv'
#     logger.info(f'create normalized_csv... > {outfile}')
#     df = df.apply(_normalize, axis=1)

#     # エラー以外のレコードをnormalized_csvとして保存
#     error_df = df[df['_ERROR'].notnull()]                   # エラーレコード(_ERROR列に値を含む)を分離
#     df = df[df['_ERROR'].isnull()].drop(columns='_ERROR')   # エラーレコード以外を取得し、_ERROR列は削除
#     df['genre_code'] = df['genre_code'].astype(int)
#     df.to_csv(outfile, columns=NORMALIZED_CSV_EXPORT_FIELDS, index=False)
#     logger.info(f'success.')

#     # 書き込み

#     # 出力先の準備
#     logger.info(f'create geojson...')
#     output_dir = pathlib.Path.cwd() / f'../data/geojson/{base}'
#     if cleanup:
#         shutil.rmtree(output_dir, ignore_errors=True)
#     output_dir_debug = output_dir / '_debug'
#     output_dir_debug.mkdir(parents=True, exist_ok=True)

#     # ジャンル別でgeojsonを出力
#     for genre_code in sorted(df['genre_code'].unique()):
#         logger.info(f'genre_code={genre_code}')
#         geojson_name = f'genre{genre_code}.geojson'
#         write_geojson(df[df['genre_code'] == genre_code], outfile=output_dir / geojson_name, debug=False)
#         write_geojson(df[df['genre_code'] == genre_code], outfile=output_dir_debug / geojson_name,)

#     # エラーレコードがあれば、エラー確認用jsonを出力
#     if not error_df.empty:
#         error_df.to_json(output_dir / '_error.json', \
#             orient='records', lines=True, force_ascii=False)

class Csv2GeoJSON:
    def __init__(self, infile):
        # TODO: 例外処理とか
        self.infile = infile
        self._parse()

    def _write_geojson(self, dest, features: list, debug):
        feature_collection = FeatureCollection(features=features)
        with open(dest, 'w', encoding='utf-8') as f:
            if debug:
                # デバッグ用GeoJsonはprettyして出力
                json.dump(feature_collection, f, ensure_ascii=False, indent=4)
            else:
                # 本番用GeoJsonは転送量削減のため、インデントと改行なしで出力
                json.dump(feature_collection, f, ensure_ascii=False, separators=(',', ':'))


    def _parse(self):
        logger.debug(f'infile={self.infile}')
        # CSVの全カラムを文字列として読み込む
        # MEMO: 書式によってはtel, zip_codeなどがintで認識されるため(例: 09012345678、書式は各都道府県のサイト依存)
        # 基本的にcrawler側では書式変換せず、(必要であれば)csv2geojson側で書式変換を行う方針とする(現状は特にやってない)
        df = pd.read_csv(self.infile, encoding="utf-8", dtype=str).fillna('')
        logger.debug('  入力レコード数= {}'.format(len(df)))

        # 読み込んだCSVデータの重複レコードチェック(店名 and 住所)
        # データチェックとして十分とは言えないが、参考程度に
        # MEMO: 以下のパターンがあるので、店名および住所単体だと重複判定できない。
        # ・同じ店名の別の店(例: 愛知県の「すずや」)
        # ・同じ住所の別の店(例: 同じショッピングモール内)
        duplicated_records = df[df.duplicated(subset=['shop_name', 'address'])]
        if not duplicated_records.empty:
            logger.warning('以下のレコードが重複しています')
            logger.warning(duplicated_records)
            # 重複行の削除
            ## MEMO: データ重複は基本的には公式サイト側の問題なので、重複削除したあと処理を続行
            df.drop_duplicates(subset=['shop_name', 'address'], keep='last', inplace=True)
        self.duplicated_records = duplicated_records

        # 正規化処理とジオコーディング
        # (失敗した場合は_ERROR列に値が入るので、その行はエラーコードとして処理)
        logger.info(f'normalize...')
        df = df.apply(_normalize, axis=1)
        # df['genre_code'] = df['genre_code'].astype(int)
        self.error_df = df[df['_ERROR'].notnull()]                              # エラーレコードを取得
        self.normalized_df = df[df['_ERROR'].isnull()].drop(columns='_ERROR')   # エラーレコード以外を取得、_ERROR列は削除


    def write_normalized_csv(self, dest):
        logger.info(f'create normalized_csv ...')
        self.normalized_df.to_csv(dest, columns=NORMALIZED_CSV_EXPORT_FIELDS, index=False)
        logger.debug('  レコード数= {}'.format(len(self.normalized_df)))

    def write_error_json(self, dest):
        logger.info(f'create error.json ...')
        df = self.error_df
        if df.empty:
            logger.debug(f'  エラーレコードなし、error_jsonは出力されません')
            return
        logger.debug('  エラーレコード数= {}'.format(len(df)))
        df.to_json(dest, orient='records', lines=True, force_ascii=False)

    def write_genred_geojson(self, output_dir, debug=False):
        logger.info('create {}geojson ...'.format('**_debug** ' if debug else ''))
        # ジャンル別でgeojsonを出力
        for genre_code in sorted(self.normalized_df['genre_code'].unique()):
            outfile = output_dir / f'genre{genre_code}.geojson'
            df = self.normalized_df[self.normalized_df['genre_code'] == genre_code]
            logger.debug('  genre{}= {}'.format(genre_code, len(df)))

            # GeoJSONのFeatureCollection要素, Feature要素を作成
            # @see https://pypi.org/project/geojson/#featurecollection
            features = df.apply(_make_feature, axis=1, debug=debug).tolist()
            self._write_geojson(outfile, features, debug)


    def write_all(self, output_dir, cleanup=True):
        # 出力先の準備
        if cleanup:
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir_debug = output_dir / '_debug'
        output_dir_debug.mkdir(parents=True, exist_ok=True)

        # こんなかんじの
        # - data/output/tochigi/ (= output_dir)
        #   - normalized.csv
        #   - _error.json
        #   - genre[1-10].geojson
        #   - _debug/genre[1-10].geojson

        self.write_normalized_csv(output_dir / 'normalized.csv')
        self.write_error_json(output_dir / '_error.json')
        self.write_genred_geojson(output_dir)
        self.write_genred_geojson(output_dir_debug, debug=True)



if __name__ == "__main__":
    # usage:
    # $ docker-compose run csv2geojson python /app/main.py --base yamanashi

    # GeoJSONのFeatureを作成するサンプル
    # addressからジオコーディングでlat,lngを取得
    rawdata = pd.Series({'address': '栃木県足利市上渋垂町字伊勢宮364-1',
        'genre_name': 'ラーメン・餃子',
        'official_page': 'https://www.kourakuen.co.jp/',
        'shop_name': '幸楽苑 足利店',
        'tel': '0284-70-5620',
        'zip_code': '326-0335'})
    normalized = _normalize(rawdata)
    logger.debug(normalized)
    feature = _make_feature(normalized)

    assert feature.properties['shop_name'] == '幸楽苑 足利店'
    assert feature.properties['genre_code'] == 5
    assert feature.properties['address'] == '栃木県足利市上渋垂町字伊勢宮364-1'
    assert feature.geometry.type == 'Point'
    assert feature.geometry.coordinates == [139.465439, 36.301109], "latlng did not match."

    # TODO: もう少し真面目に、かつクラスにしてあげたい
    # argsもinとoutをそれぞれ指定してあげれば良さそう
    # 例: --infile data/infile/tochigi.csv --outdir data/outfile/tochigi/
    parser = argparse.ArgumentParser(description='goto-eat-csv2geojson')
    parser.add_argument('--base', help='例: tochigi')   # 個別指定オプション
    args = parser.parse_args()
    base = args.base

    if not base:
        # やる気のない全件処理(動作未確認)
        target = pathlib.Path.cwd() / f'../data/csv/'
        for infile in list(target.glob('*.csv')):
            output_dir = pathlib.Path.cwd() / f'../data/output/{infile.stem}'
            parser = Csv2GeoJSON()
            parser.parse(infile)
            parser.write_all(output_dir)

    else:
        infile = pathlib.Path.cwd() / f'../data/csv/{base}.csv'
        output_dir = pathlib.Path.cwd() / f'../data/output/{base}'
        parser = Csv2GeoJSON()
        parser.parse(infile)
        parser.write_all(output_dir)

