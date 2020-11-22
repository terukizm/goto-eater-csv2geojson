
import pandas as pd
from logzero import logger
from geojson import Feature, FeatureCollection, Point
import argparse

import csv
import json
import pathlib
from collections import OrderedDict
from urllib.parse import quote

from util import geocode, normalize, NormalizeError, GeocodeError
from pprint import pprint
from pydams import DAMS

DAMS.init_dams()
debug = True

def _make_feature(row: pd.Series):
    """
    GeoJSONのFeature要素(POINT限定)を作成
    @see https://pypi.org/project/geojson/#point
    """
    shop_name = row['shop_name']
    address = row['address']
    try:
        normalized_address = normalize(address)
        lat, lng, _debug = geocode(address)
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

    properties = OrderedDict(row)
    properties['GoogleMap'] = 'https://www.google.com/maps/search/?q=' + quote(f'{normalized_address} {shop_name}')
    # 以下はデバッグ情報
    if debug:
        properties['_ジオコーディング対象の住所(ビル名等を取り除いた入力文字列)'] = normalized_address
        properties['_ジオコーディングの結果スコア'] = _debug[0]
        properties['_ジオコーディング結果に紐づく住所情報(name)'] = _debug[1]
        properties['_ジオコーディングで無視された住所情報(tail)'] = _debug[2]
        properties['_国土地理院地図のURL'] = f'https://maps.gsi.go.jp/#17/{lat}/{lng}/'

    # print(f'🌏  {lat}, {lng}')
    # logger.debug(_debug)
    # pprint(properties)
    # import sys
    # sys.exit(0)

    return Feature(geometry=Point((lng, lat)), properties=properties)


def write_geojson(df: pd.DataFrame, outfile: str):
    """
    CSVの内容(df形式)を元にgeojsonを出力
    """
    # GeoJSONのFeatureCollection要素, Feature要素を作成
    # @see https://pypi.org/project/geojson/#featurecollection
    features = df.apply(_make_feature, axis=1).tolist()
    feature_collection = FeatureCollection(features=features)

    with open(outfile, 'w', encoding='utf-8') as f:
        indent = 4 if debug else 0  # TODO: 本番用はインデントなしにして容量削減
        json.dump(feature_collection, f, ensure_ascii=False, indent=indent)


if __name__ == "__main__":
    # usage:
    # $ docker-compose run csv2geojson python /app/main.py 19_yamanashi

    # GeoJSONのFeatureを作成するサンプル
    # addressからジオコーディングでlat,lngを取得
    a = {'address': '栃木県足利市上渋垂町字伊勢宮364-1',
        'genre_name': 'ラーメン・餃子',
        'offical_page': 'https://www.kourakuen.co.jp/',
        'shop_name': '幸楽苑 足利店',
        'tel': '0284-70-5620',
        'zip_code': '326-0335'}
    feature = _make_feature(pd.Series(a))
    assert feature.properties['shop_name'] == '幸楽苑 足利店'
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

    # 読み込み
    logger.info(f'base={base}')
    infile = pathlib.Path.cwd() / f'../data/csv/{base}.csv'
    df = pd.read_csv(infile, encoding="utf-8", \
        dtype={'shop_name': str, 'tel': str}) \
        .fillna({'shop_name': '', 'address': '', 'offical_page': '', 'tel': '', 'zip_code': '', 'genre_name': 'その他'})

    # 読み込んだデータの重複レコードチェック(店名 and 住所)
    # 店名、住所を個別で実行することも可能だが、
    # 以下の理由で店名、住所は単体だとduplicate判定できない場合がある
    # ・同じ店名(例: 愛知県の「すずや」)
    # ・同じショッピングモール内
    # 入力ミスのパターンによっては十分とは言えないが、参考程度にチェックを入れておく
    duplicated_records = df[df.duplicated(subset=['shop_name', 'address'])]
    if not duplicated_records.empty:
        logger.warn('以下のレコードが重複しています')
        logger.warn(duplicated_records)


    # 書き込み
    outfile = pathlib.Path.cwd() / f'../data/geojson/{base}_all.geojson'
    logger.info(f'genre_name=all')
    write_geojson(df, outfile)

    # ジャンル別で分割出力
    # for genre_name in df['genre_name'].unique():
    #     # FIXME・ジャンル名に/を入れてるやつの暫定対応
    #     outfile = pathlib.Path.cwd() / '../data/geojson/{}_{}.geojson'.format(base, genre_name.replace('/', '／'))
    #     logger.info(f'genre_name={genre_name}')
    #     genred_df = df[df['genre_name'] == genre_name]
    #     write_geojson(genred_df, outfile)

    # "|"区切りの複数ジャンルに対応したジャンル別出力
    # FIXME: 雑・ジャンル名に/を入れてるやつの暫定対応
    genre_list = {}
    for _, row in df.iterrows():
        for genre in row['genre_name'].split('|'):
            if not genre_list.get(genre):
                genre_list[genre] = []
            genre_list[genre].append(row)

    for genre_name, row in genre_list.items():
        outfile = pathlib.Path.cwd() / '../data/geojson/{}_{}.geojson'.format(base, genre_name.replace('/', '／'))
        logger.info(f'genre_name={genre_name}')
        write_geojson(pd.DataFrame(row), outfile)
