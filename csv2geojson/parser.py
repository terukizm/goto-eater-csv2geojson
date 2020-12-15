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

from csv2geojson import util, genre

def normalize_and_geocode(row: pd.Series, pref_name: str, zip_code_validation=False):
    """
    ジャンル名と住所を正規化し、ジオコーディングで取得した地理情報と合わせてGeoJSONの1Pointに相当するpd.Seriresを生成
    """
    # ジャンル再分類
    try:
        genre_code = genre.classify(row['genre_name'])
        row['genre_code'] = genre_code
    except genre.GenreNotFoundError as e:
        logger.warning('{}: {}'.format(e, row.to_dict()))
        row['genre_code'] = genre.GENRE_その他

    # 公式サイトからlatlngが提供されている場合は優先してそちらを利用
    # この場合は住所の正規化やジオコーディングを行う必要がない
    if row['provided_lat'] and row['provided_lng']:
        lat = float(row['provided_lat'])
        lng = float(row['provided_lng'])
        row['lat'] = lat
        row['lng'] = lng
        row['normalized_address'] = ''
        row['_dams_score'] = ''
        row['_dams_name'] = ''
        row['_dams_tail'] = ''
        row['_ERROR'] = np.nan

        # その他の補足情報を追加
        googlemap_q_string = '{} {}'.format(row['address'], row['shop_name'])
        row['google_map_url'] = 'https://www.google.com/maps/search/?q=' + quote(googlemap_q_string)
        row['_gsi_map_url'] = 'https://maps.gsi.go.jp/#17/{}/{}/'.format(lat, lng)

        return row

    # 住所の正規化、ジオコーディング
    try:
        address = row['address']
        normalized_address = util.normalize_for_pydams(address, pref_name)
        lat, lng, _debug = util.geocode_with_pydams(normalized_address)

        # 郵便番号でのジオコーディング結果に対する正当性チェック(任意)
        if zip_code_validation:
            util.validate_by_zipcode(zip_code=row['zip_code'], address=normalized_address)
        # MEMO: 簡易的なものであり、「愛知県名古屋市xxxx」を「名古屋xxxx」と誤入力されたことで、
        # 千葉県成田市、新潟県佐渡市にある地名の「名古屋」のように、明らかに誤ったジオコーディング結果になっている場合に
        # 検出できる、という程度のもの。posutoを用いているが、結果は県名までしか利用していない。
        # (posuto.city, posuto.neighborhood の値も使えることは使えるが、pydamsが返す住所形式とposutoが返す結果の住所形式が異なることがあるため。
        #  例: pydams = "栃木県/塩谷郡/高根沢町",  posuto = "栃木県/高根沢町"
        # どちらも住所形式としては正しく、どちらも同じ住所を指すが、こういった郡とか字(大字)の正規化まで考えると頭がおかしくなってくるので)
        # なお任意としているのは、posutoの実装が内部的にsqlite3を使っており、とにかく遅いため。
        # (栃木の3500件程度で off=1min, on=4.3min くらいの差がつく)
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

    except (util.NormalizeError, util.GeocodeError) as e:
        # 住所の正規化エラー(NormalizeError), ジオコーディングエラー(GeocodeError)が発生した場合、
        # dfの_ERROR列に発生したエラー名を追加、後から追えるようにしておく
        name = e.__class__.__name__
        row['_ERROR'] = name
        logger.warning('{}: {}'.format(name, row.to_dict()))
        logger.warning(e)
    except util.ZipCodeValidationError as e:
        # 郵便番号チェックエラー(ZipCodeValidationError)が発生した場合、
        # 以下のようなケースがあり、一概にエラーであると処理できないので、ログ出力だけとする
        # 1. ジオコーディングに失敗しており、間違った住所で、latlngが求められている (正規化が不十分 or ジオコーディングに失敗している)
        #   => このケースを想定して実装したが、ほとんどは入力データ形式が不十分なこと(郵便番号で補完できる区名や市名を省略しているなど)
        #      に起因しており、かなり面倒くさい (対応する場合、正常データも含めて住所自体のきちんとした正規化が必要になってくる)
        # 2. 郵便番号がそもそも間違っている (元データが悪い) => どうしようもない、住所の方が正しければ(まあ)問題はない
        # 3. 郵便番号がそもそも「複数の都道府県にまたがる」郵便番号である (例: "498-0000") => 郵便番号の仕様、問題はない
        name = e.__class__.__name__
        logger.warning('📮 {}: {}'.format(name, row.to_dict()))
        logger.warning(e)
    except Exception as e:
        # それ以外の例外が発生した場合にはコケさせる
        logger.error('{}: {}'.format(e.__class__.__name__, row.to_dict()))
        logger.exception(e)
        raise

    return row

def make_feature(row: pd.Series, debug=False):
    """
    GeoJSONのFeature要素(POINT限定)を作成
    @see https://pypi.org/project/geojson/#point
    """
    try:
        # debugオプションの有無でprefixに'_'がついた項目を出し分け
        props = OrderedDict(row) if debug \
            else OrderedDict({k: v for k, v in OrderedDict(row).items() if not k.startswith('_') })

        # lat, lngは「properites」ではなく「geometry」に配置
        lat = props.pop('lat')
        lng = props.pop('lng')
    except Exception as e:
        logger.error(props)
        raise

    coords = (lng, lat) # MEMO: lng, latの順番に注意
    return Feature(geometry=Point(coords), properties=props)

def write_geojson(dest, df: pd.DataFrame, debug=False):
    """
    GeoJSONファイルを作成
    """
    # GeoJSONのFeatureCollection要素, Feature要素を作成
    # @see https://pypi.org/project/geojson/#featurecollection
    logger.debug('  レコード数= {}'.format(len(df)))
    features = df.apply(make_feature, axis=1, debug=debug).tolist()
    feature_collection = FeatureCollection(features=features)
    with open(dest, 'w', encoding='utf-8') as f:
        if debug:
            # デバッグ用GeoJSONは読みやすいように、prettyして出力
            json.dump(feature_collection, f, ensure_ascii=False, indent=4)
        else:
            # 本番用GeoJSONはデータサイズ削減のため、minifyして出力
            json.dump(feature_collection, f, ensure_ascii=False, separators=(',', ':'))


class Csv2GeoJSON:
    # @see goto_eat_scrapy.settings.FEED_EXPORT_FIELDS
    FEED_EXPORT_FIELDS = ['shop_name', 'address', 'tel', 'genre_name', 'zip_code', 'official_page', 'opening_hours', 'closing_day', 'area_name', 'detail_page', 'provided_lat', 'provided_lng']
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

    def __init__(self, src: pathlib.Path, zip_code_validation=False):
        self.zip_code_validation = zip_code_validation
        self._parse(src)

    def _parse(self, src: pathlib.Path):
        logger.debug(f'入力CSV={src}')

        # CSVの全カラムを文字列として読み込む
        # MEMO: 書式によってはtel, zip_codeなどがintで認識される(例: "09012345678"、書式は各都道府県のサイトに依存)
        # 基本的にcrawler側では書式変換せず、(必要であれば)csv2geojson側で書式変換を行う方針。
        # (現状は特に処理していないが、必要なら郵便番号や電話番号のフォーマットを統一したりしてもよい)
        df = pd.read_csv(src, encoding="utf-8", dtype=str).fillna('')
        logger.debug('  入力レコード数= {}'.format(len(df)))

        # 読み込んだCSVデータの重複レコードチェック(店名 and 住所)
        # データチェックとして十分とは言えないが、参考程度に
        # MEMO: 以下のパターンがあるので、店名および住所単体だと重複判定できない。
        # ・同じ店名の別の店(例: 愛知県の「すずや」)
        # ・同じ住所の別の店(例: 同じショッピングモール内)
        duplicated_records = df[df.duplicated(subset=['shop_name', 'address'])]
        if not duplicated_records.empty:
            # 重複行の削除
            ## MEMO: データ重複は(クローリングに実装ミスがなければ)公式サイト側の問題なので、重複削除したあとに処理を続行
            df.drop_duplicates(subset=['shop_name', 'address'], keep='last', inplace=True)
        self.duplicated_df = duplicated_records

        # 正規化処理とジオコーディング
        # (失敗した場合は_ERROR列に値が入るので、その行はエラーレコードとして処理)
        logger.info(f'normalize...')
        df = df.apply(normalize_and_geocode, axis=1, pref_name=src.stem, zip_code_validation=self.zip_code_validation)
        self.error_df = df[df['_ERROR'].notnull()]                              # エラーレコードを取得
        self.normalized_df = df[df['_ERROR'].isnull()].drop(columns='_ERROR')   # エラーレコード以外を取得、_ERROR列は削除

    def write_normalized_csv(self, dest):
        """
        正規化、地理情報等を付与したCSVを出力
        """
        logger.info('create normalized_csv ...')
        self.normalized_df.to_csv(dest, columns=self.NORMALIZED_CSV_EXPORT_FIELDS, index=False)
        logger.debug('  レコード数={}'.format(len(self.normalized_df)))

    def write_all_geojson(self, dest, debug=False):
        """
        ジャンル分けなしでGeoJSONを出力
        """
        logger.info('create all_geojson {} ...'.format('<_debug> ' if debug else ''))
        write_geojson(dest, self.normalized_df, debug)

    def write_genred_geojson(self, output_dir, debug=False):
        """
        ジャンル別でGeoJSONを出力
        """
        for genre_code in sorted(self.normalized_df['genre_code'].unique()):
            outfile = output_dir / f'genre{genre_code}.geojson'
            df = self.normalized_df[self.normalized_df['genre_code'] == genre_code]
            logger.info('create genre{}_geojson {} ...'.format(genre_code, '<_debug> ' if debug else ''))
            write_geojson(outfile, df, debug)

    def write_error_json(self, dest):
        """
        エラー確認用JSONを出力
        """
        logger.info(f'create _error.json ...')
        logger.debug('  重複レコード数= {}'.format(len(self.duplicated_df)))
        logger.debug('  エラーレコード数= {}'.format(len(self.error_df)))
        if len(self.duplicated_df) == 0 and len(self.error_df) == 0:
            # 重複レコード、エラーコードともに存在しない場合、JSON出力しない
            return
        data = {
            'duplicated': self.duplicated_df.fillna('').to_dict(orient='records'),
            'error': self.error_df.fillna('').to_dict(orient='records'),
        }
        with open(dest, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)    # pretty

    def write_all(self, output_dir, cleanup=True):
        """
        各種成果物の一括生成
        """
        # 出力先の準備
        if cleanup:
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir_debug = output_dir / '_debug'
        output_dir_debug.mkdir(parents=True, exist_ok=True)

        self.write_normalized_csv(output_dir / 'normalized.csv')
        self.write_error_json(output_dir / '_error.json')
        self.write_all_geojson(output_dir / 'all.geojson')
        self.write_all_geojson(output_dir_debug / 'all.geojson', debug=True)
        self.write_genred_geojson(output_dir)
        self.write_genred_geojson(output_dir_debug, debug=True)


if __name__ == "__main__":
    # usage:
    # $ docker-compose run csv2geojson python -m csv2geojson.parser

    # GeoJSONのFeatureを作成するサンプル
    # addressからジオコーディングでlat,lngを取得
    rawdata = pd.Series({
        'address': '足利市上渋垂町字伊勢宮364-1 なんとかビル1F',
        'genre_name': 'ラーメン・餃子',
        'official_page': 'https://www.kourakuen.co.jp/',
        'shop_name': '幸楽苑 足利店',
        'tel': '0284-70-5620',
        'zip_code': '326-0335',
    })
    normalized_data = normalize_and_geocode(rawdata, pref_name='tochigi')
    feature = make_feature(normalized_data, debug=True)

    assert feature.properties['shop_name'] == '幸楽苑 足利店'
    assert feature.properties['genre_code'] == genre.GENRE_麺類
    assert feature.properties['address'] == '足利市上渋垂町字伊勢宮364-1 なんとかビル1F'
    assert feature.properties['normalized_address'] == '栃木県足利市上渋垂町字伊勢宮364-1'
    assert feature.properties['_dams_score'] == 5
    assert feature.properties['_dams_name'] == '栃木県足利市上渋垂町'
    assert feature.properties['_dams_tail'] == '伊勢宮364-1'
    assert feature.geometry.type == 'Point'
    assert feature.geometry.coordinates == [139.465439, 36.301109], "latlng did not match."

    print('success!!')
