
import pandas as pd
from geojson import Feature, FeatureCollection, Point
import argparse
import logging
import logzero
from logzero import logger

import csv
import json
import pathlib
from collections import OrderedDict
from urllib.parse import quote

from util import geocode, normalize, normalize_genre_by_code, NormalizeError, GeocodeError
from pydams import DAMS

DAMS.init_dams()

def _make_feature(row: pd.Series, debug=True):
    """
    GeoJSONのFeature要素(POINT限定)を作成
    @see https://pypi.org/project/geojson/#point
    """
    try:
        properties = OrderedDict(row) if debug \
            else OrderedDict({k: v for k, v in OrderedDict(row).items() if not k.startswith('_') })
        lat = properties.pop('lat')
        lng = properties.pop('lng')

        # 補足情報を追加
        google_map_args = properties['normalized_address'] + ' ' + properties['shop_name']
        properties['GoogleMap'] = 'https://www.google.com/maps/search/?q=' + quote(google_map_args)
        if debug:
            properties['_国土地理院地図のURL'] = f'https://maps.gsi.go.jp/#17/{lat}/{lng}/'
    except Exception as e:
        logger.error(properties)
        raise e

    return Feature(geometry=Point((lng, lat)), properties=properties)


def _normalize(row: pd.Series):
    """
    ・カテゴリ名
    ・住所名
    """
    shop_name = row['shop_name']
    address = row['address']
    try:
        normalized_address = normalize(address)
        lat, lng, _debug = geocode(address)
        genre_code = normalize_genre_by_code(row['genre_name'])
    except NormalizeError as e:
        logger.error("NormalizeError:")
        logger.error(row.to_dict())
        return False
    except GeocodeError as e:
        logger.error("GeocodeError:")
        logger.error(row.to_dict())
        return False
    except Exception as e:
        logger.error("Other Exception. Whats????")
        logger.error(e)
        logger.error(row.to_dict())
        return False

    row['lat'] = lat
    row['lng'] = lng
    row['genre_code'] = genre_code
    row['normalized_address'] = normalized_address
    row['_ジオコーディングの結果スコア'] = _debug[0]
    row['_ジオコーディング結果に紐づく住所情報(name)'] = _debug[1]
    row['_ジオコーディングで無視された住所情報(tail)'] = _debug[2]

    return row


def write_geojson(df: pd.DataFrame, outfile: str, debug=True):
    """
    CSVの内容(df形式)を元にgeojsonを出力
    """
    # GeoJSONのFeatureCollection要素, Feature要素を作成
    # @see https://pypi.org/project/geojson/#featurecollection
    features = df.apply(_make_feature, axis=1, debug=debug).tolist()
    feature_collection = FeatureCollection(features=features)

    with open(outfile, 'w', encoding='utf-8') as f:
        if debug:
            json.dump(feature_collection, f, ensure_ascii=False, indent=4)
        else:
            # 本番用はインデントなしにして、ファイルサイズを削減
            json.dump(feature_collection, f, ensure_ascii=False, separators=(',', ':'))

if __name__ == "__main__":
    # usage:
    # $ docker-compose run csv2geojson python /app/main.py 19_yamanashi

    # GeoJSONのFeatureを作成するサンプル
    # addressからジオコーディングでlat,lngを取得
    rawdata = pd.Series({'address': '栃木県足利市上渋垂町字伊勢宮364-1',
        'genre_name': 'ラーメン・餃子',
        'offical_page': 'https://www.kourakuen.co.jp/',
        'shop_name': '幸楽苑 足利店',
        'tel': '0284-70-5620',
        'zip_code': '326-0335'})
    normalized = _normalize(rawdata)
    feature = _make_feature(normalized)

    assert feature.properties['shop_name'] == '幸楽苑 足利店'
    assert feature.properties['genre_code'] == 5
    assert feature.properties['address'] == '栃木県足利市上渋垂町字伊勢宮364-1'
    assert feature.geometry.type == 'Point'
    assert feature.geometry.coordinates == [139.465439, 36.301109], "latlng did not match."

    # TODO: もう少し真面目にやってもよい
    parser = argparse.ArgumentParser(description='goto-eat-csv2geojson')
    parser.add_argument('base', help='e.g.: 09_tochigi')
    args = parser.parse_args()

    base = '09_tochigi'
    base = '27_osaka'   # インデントなしのデバッグ情報なしで9MBくらい、許容範囲？
    base = '11_saitama'
    base = '19_yamanashi'   # 軽い
    base = '10_gunma'
    base = args.base

    # CSV読み込み
    logger.info(f'base={base}')
    infile = pathlib.Path.cwd() / f'../data/csv/{base}.csv'
    df = pd.read_csv(infile, encoding="utf-8", \
        dtype={'shop_name': str, 'tel': str}) \
        .fillna({'shop_name': '', 'address': '', 'offical_page': '', 'tel': '', 'zip_code': '', 'genre_name': ''})

    # 読み込んだCSVデータの重複レコードチェック(店名 and 住所)
    # 店名、住所を個別で実行することも可能だが、以下の理由で店名、住所は単体だと重複判定できない場合がある
    # ・同じ店名の別の店(例: 愛知県の「すずや」)
    # ・同じショッピングモール内にある別の店
    # 入力ミスのパターンによっては十分とは言えないが、参考程度にチェックを入れておく
    duplicated_records = df[df.duplicated(subset=['shop_name', 'address'])]
    if not duplicated_records.empty:
        logger.warn('以下のレコードが重複しています')
        logger.warn(duplicated_records)

    # normalized_csvの作成
    # ・カテゴリ名
    # ・住所名
    # 上記２つを正規化し、さらにデバッグ用情報を付与したもの
    outfile = pathlib.Path.cwd() / f'../data/normalized_csv/{base}.csv'
    logger.info(f'create normalized_csv... > {outfile}')
    df = df.apply(_normalize, axis=1)
    df.to_csv(outfile, index=False)
    logger.info(f'success.')

    # 書き込み

    # ディレクトリ準備
    logger.info(f'create geojson...')
    output_dir = pathlib.Path.cwd() / f'../data/geojson/{base}'
    output_dir_debug = output_dir / '_debug'
    output_dir_debug.mkdir(parents=True, exist_ok=True)

    # ジャンル別でgeojsonを出力
    for genre_code in sorted(df['genre_code'].unique()):
        logger.info(f'genre_code={genre_code}')
        geojson_name = f'genre{genre_code}.geojson'
        write_geojson(df[df['genre_code'] == genre_code], outfile=output_dir / geojson_name, debug=False)
        write_geojson(df[df['genre_code'] == genre_code], outfile=output_dir_debug / geojson_name,)


    ## _all (必要ないかも), 複数ジャンル対応と合わせて後で消す

    # logzero.loglevel(logging.ERROR)  # allとそれ以外のジャンルとで重複してerrorが出るので
    # outfile = pathlib.Path.cwd() / f'../data/geojson/{base}_all.geojson'
    # logger.info(f'genre_name=all')
    # write_geojson(df, outfile)
    # logzero.loglevel(logging.INFO)

    # # "|"区切りの複数ジャンルに対応したジャンル別出力
    # # FIXME: 雑・ジャンル名に/を入れてるやつの暫定対応
    # genre_list = {}
    # for _, row in df.iterrows():
    #     for genre in row['genre_name'].split('|'):
    #         if not genre_list.get(genre):
    #             genre_list[genre] = []
    #         genre_list[genre].append(row)

    # for genre_name, row in genre_list.items():
    #     outfile = pathlib.Path.cwd() / '../data/geojson/{}_{}.geojson'.format(base, genre_name.replace('/', '／'))
    #     logger.info(f'genre_name={genre_name}')
    #     write_geojson(pd.DataFrame(row), outfile)
