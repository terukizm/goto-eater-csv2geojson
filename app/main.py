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
from geojson import Feature, FeatureCollection, Point

from util import geocode, normalize, NormalizeError, GeocodeError
import genre

# @see goto_eat_scrapy.settings.FEED_EXPORT_FIELDS
FEED_EXPORT_FIELDS = ['shop_name', 'address', 'tel', 'genre_name', 'zip_code', 'official_page', 'opening_hours', 'closing_day', 'area_name', 'detail_page']

NORMALIZED_CSV_EXPORT_FIELDS = FEED_EXPORT_FIELDS + [
    'lat',
    'lng',
    'normalized_address',
    'genre_code',
    'google_map_url',
    '_gsi_map_url',
    '_dams_score',
    '_dams_name',
    '_dams_tail',
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
    except Exception as e:
        logger.error(properties)
        raise e

    coords = (lng, lat) # MEMO: lng, latの順番に注意
    return Feature(geometry=Point(coords), properties=properties)


def _normalize(row: pd.Series):
    """
    ・カテゴリ名
    ・住所名
    """
    try:
        # ジャンル再分類
        try:
            genre_code = genre.classify(row['genre_name'])
            row['genre_code'] = genre_code
        except genre.GenreNotFoundError as e:
            logger.warning('{}: {}'.format(e, row.to_dict()))
            row['genre_code'] = genre.GENRE_その他

        # 住所の正規化、ジオコーディング
        address = row['address']
        normalized_address = normalize(address)
        lat, lng, _debug = geocode(address)

        row['lat'] = lat
        row['lng'] = lng
        row['normalized_address'] = normalized_address
        row['_dams_score'] = _debug[0]
        row['_dams_name'] = _debug[1]
        row['_dams_tail'] = _debug[2]
        row['_ERROR'] = np.nan

        # その他の補足情報を追加
        googlemap_q_string = '{} {}'.format(normalized_address, row['shop_name'])
        row['google_map_url'] = 'https://www.google.com/maps/search/?q=' + quote(googlemap_q_string)
        row['_gsi_map_url'] = f'https://maps.gsi.go.jp/#17/{lat}/{lng}/'

        ## -> 量が多いので一時的にコメントアウト…
        # if row['_dams_score'] < 5:
        #     # MEMO: DAMSのscoreが4以下の場合、複数のジオコーディング結果が得られた可能性がある
        #     # @see http://newspat.csis.u-tokyo.ac.jp/geocode/modules/dams/index.php?content_id=4
        #     # GeocodeErrorとは違って一概に「だからlatlngの値がダメ」とはできないため、参考程度にログに出しておく
        #     logger.info('  (low dams_score): {}'.format(row.to_dict()))

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


class Csv2GeoJSON:
    def __init__(self, infile):
        self.infile = infile
        self._parse()

    def _write_geojson(self, dest, df: pd.DataFrame, debug):
        # GeoJSONのFeatureCollection要素, Feature要素を作成
        # @see https://pypi.org/project/geojson/#featurecollection
        features = df.apply(_make_feature, axis=1, debug=debug).tolist()
        feature_collection = FeatureCollection(features=features)
        logger.debug('  レコード数= {}'.format(len(df)))
        with open(dest, 'w', encoding='utf-8') as f:
            if debug:
                # デバッグ用GeoJSONは読みやすいように、prettyして出力
                json.dump(feature_collection, f, ensure_ascii=False, indent=4)
            else:
                # 本番用GeoJSONはデータサイズ削減のため、minifyして出力
                json.dump(feature_collection, f, ensure_ascii=False, separators=(',', ':'))

    def _parse(self):
        logger.debug(f'infile={self.infile}')
        # CSVの全カラムを文字列として読み込む
        # MEMO: 書式によってはtel, zip_codeなどがintで認識される(例: "09012345678"、書式は各都道府県のサイトに依存)
        # 基本的にcrawler側では書式変換せず、(必要であれば)csv2geojson側で書式変換を行う方針
        # (現状は特にやってないが、必要になったら郵便番号や電話番号のフォーマットを統一したりしてもよい)
        df = pd.read_csv(self.infile, encoding="utf-8", dtype=str).fillna('')
        logger.debug('  入力レコード数= {}'.format(len(df)))

        # 読み込んだCSVデータの重複レコードチェック(店名 and 住所)
        # データチェックとして十分とは言えないが、参考程度に
        # MEMO: 以下のパターンがあるので、店名および住所単体だと重複判定できない。
        # ・同じ店名の別の店(例: 愛知県の「すずや」)
        # ・同じ住所の別の店(例: 同じショッピングモール内)
        duplicated_records = df[df.duplicated(subset=['shop_name', 'address'])]
        if not duplicated_records.empty:
            # 重複行の削除
            ## MEMO: データ重複は基本的には公式サイト側のデータ入力等の問題なので、重複削除したあとに処理を続行
            df.drop_duplicates(subset=['shop_name', 'address'], keep='last', inplace=True)
        self.duplicated_df = duplicated_records

        # 正規化処理とジオコーディング
        # (失敗した場合は_ERROR列に値が入るので、その行はエラーコードとして処理)
        logger.info(f'normalize...')
        df = df.apply(_normalize, axis=1)
        self.error_df = df[df['_ERROR'].notnull()]                              # エラーレコードを取得
        self.normalized_df = df[df['_ERROR'].isnull()].drop(columns='_ERROR')   # エラーレコード以外を取得、_ERROR列は削除

    def write_normalized_csv(self, dest):
        logger.info('create normalized_csv ...')
        self.normalized_df.to_csv(dest, columns=NORMALIZED_CSV_EXPORT_FIELDS, index=False)
        logger.debug('  レコード数= {}'.format(len(self.normalized_df)))

    def write_all_geojson(self, output_dir, debug=False):
        logger.info('create all_geojson {} ...'.format('<_debug> ' if debug else ''))
        outfile = output_dir / 'all.geojson'
        self._write_geojson(outfile, self.normalized_df, debug)

    def write_genred_geojson(self, output_dir, debug=False):
        # ジャンル別でgeojsonを出力
        for genre_code in sorted(self.normalized_df['genre_code'].unique()):
            outfile = output_dir / f'genre{genre_code}.geojson'
            df = self.normalized_df[self.normalized_df['genre_code'] == genre_code]
            logger.info('create genre{}_geojson {} ...'.format(genre_code, '<_debug> ' if debug else ''))
            self._write_geojson(outfile, df, debug)

    def write_error_json(self, dest):
        logger.info(f'create _error.json ...')
        logger.debug('  重複レコード数= {}'.format(len(self.duplicated_df)))
        logger.debug('  エラーレコード数= {}'.format(len(self.error_df)))
        data = {
            'duplicated': self.duplicated_df.fillna('').to_dict(orient='records'),
            'error': self.error_df.fillna('').to_dict(orient='records'),
        }
        with open(dest, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def write_all(self, output_dir, cleanup=True):
        """
        一括生成
        """
        # 出力先の準備
        if cleanup:
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir_debug = output_dir / '_debug'
        output_dir_debug.mkdir(parents=True, exist_ok=True)

        self.write_normalized_csv(output_dir / 'normalized.csv')
        self.write_error_json(output_dir / '_error.json')
        self.write_all_geojson(output_dir)
        self.write_genred_geojson(output_dir)
        self.write_all_geojson(output_dir_debug, debug=True)
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

    # TODO: もう少し真面目にしてあげたい
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
            parser = Csv2GeoJSON(infile)
            parser.write_all(output_dir)

    else:
        infile = pathlib.Path.cwd() / f'../data/csv/{base}.csv'
        output_dir = pathlib.Path.cwd() / f'../data/output/{base}'
        parser = Csv2GeoJSON(infile)
        parser.write_all(output_dir)

