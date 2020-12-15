import re
import posuto
from pydams import DAMS
from functools import lru_cache
DAMS.init_dams()

class NormalizeError(Exception):
    pass
class GeocodeError(Exception):
    pass

class ZipCodeValidationError(Exception):
    pass

# 以下の正規表現に「無番地」を追加
# @see https://qiita.com/shouta-dev/items/b87efc19e105045881de
regex = r"([0-9０-９]+|[一二三四五六七八九十百千万]+)*(([0-9０-９]+|[一二三四五六七八九十百千万]+)|(丁目|丁|無番地|番地|番|号|-|‐|－|‑|ー|−|‒|–|—|―|ｰ|の|東|西|南|北){1,2})*(([0-9０-９]+|[一二三四五六七八九十百千万]}+)|(丁目|丁|無番地|番地|番|号){1,2})"

def normalize_for_pydams(address: str, pref_name:str):
    """
    pydamsが正しくジオコーディング結果を返せる形式に住所文字列を正規化
     ・番地以下、ビル名を含むと誤爆することがあるので除去
     ・区切りスペースはあってもなくてもOK(どちらでもpydamsの結果には影響しないっぽい)
     ・丁目、番地は漢数字・半角数字・全角数字どれでもOK
     ・xx-yy形式でも、xx番地yy丁目形式でもOK
    """
    if not address:
        return ''

    # 番地部分だけ抽出
    m = re.search(regex, address)
    if not m:
        raise NormalizeError(f'  address={address}');
    addr2 = m.group()
    addr1 = address.split(addr2)[0]

    # 住所が都道府県名から始まってない場合はpref_nameで補填
    pref_ja = pref_name_ja_from_roman(pref_name)
    if not addr1.startswith(pref_ja):
        return pref_ja + addr1 + addr2

    return addr1 + addr2


def geocode_with_pydams(normalized_address: str):
    """
    PyDAMSを利用したジオコーディング
    """
    # @see http://newspat.csis.u-tokyo.ac.jp/geocode/modules/dams/index.php?content_id=4
    geocoded = DAMS.geocode_simplify(normalized_address)
    if not geocoded:
        raise GeocodeError('no geocode')

    lat = round(geocoded['candidates'][0]['y'], 6)  # 国土地理院地図に合わせて6桁とした
    lng = round(geocoded['candidates'][0]['x'], 6)

    score = geocoded['score']                   # 1〜5
    name = geocoded['candidates'][0]['name']    # ジオコーディングに寄与する住所
    tail = geocoded['tail']                     # ジオコーディングに寄与してない住所

    return (lat, lng, (score, name, tail))

def pref_name_ja_from_roman(pref_name: str):
    """
    例: pref_name_ja_from_roman(pref_name='tochigi') -> '栃木県'
    """
    # FIXME: 静岡(青券)のクソ対応
    if pref_name == 'shizuoka_blue':
        return "静岡県"

    # なんかなかったっけと思って探したけど(pycountryとか)都道府県名まで入ってるのがなかったので脳死対応
    # jsかなんかのをコピペしているので許してくれ #FIXME
    prefs = {
        "北海道": "hokkaido",
        "青森県": "aomori",
        "岩手県": "iwate",
        "宮城県": "miyagi",
        "秋田県": "akita",
        "山形県": "yamagata",
        "福島県": "fukushima",
        "茨城県": "ibaraki",
        "栃木県": "tochigi",
        "群馬県": "gunma",
        "埼玉県": "saitama",
        "千葉県": "chiba",
        "東京都": "tokyo",
        "神奈川県": "kanagawa",
        "新潟県": "niigata",
        "富山県": "toyama",
        "石川県": "ishikawa",
        "福井県": "fukui",
        "山梨県": "yamanashi",
        "長野県": "nagano",
        "岐阜県": "gifu",
        "静岡県": "shizuoka",
        "愛知県": "aichi",
        "三重県": "mie",
        "滋賀県": "shiga",
        "京都府": "kyoto",
        "大阪府": "osaka",
        "兵庫県": "hyogo",
        "奈良県": "nara",
        "和歌山県": "wakayama",
        "鳥取県": "tottori",
        "島根県": "shimane",
        "岡山県": "okayama",
        "広島県": "hiroshima",
        "山口県": "yamaguchi",
        "徳島県": "tokushima",
        "香川県": "kagawa",
        "愛媛県": "ehime",
        "高知県": "kochi",
        "福岡県": "fukuoka",
        "佐賀県": "saga",
        "長崎県": "nagasaki",
        "熊本県": "kumamoto",
        "大分県": "oita",
        "宮崎県": "miyazaki",
        "鹿児島県": "kagoshima",
        "沖縄県": "okinawa",
    }
    return [k for k, v in prefs.items() if v == pref_name][0]

@lru_cache(maxsize=None)
def cached_posuto_pref(zip_code: str):
    return posuto.get(zip_code).prefecture

def validate_by_zipcode(zip_code: str, address: str):
    if not zip_code:
        return
    try:
        pref = cached_posuto_pref(zip_code)
        if not address.startswith(pref):
            raise ZipCodeValidationError(f'pref_by_zipcode is {pref}, but address is {address}')
    except KeyError as e:
        # MEMO: posutoのデータには存在しない(特殊な)郵便番号が指定されている場合がある
        # その場合は仕方ないのでバリデーション成功とする
        pass

if __name__ == "__main__":
    # pydams入ってないと動かないので(本当はmockしてあげたりするとよい)
    # $ docker-compose run csv2geojson python -m csv2geojson.util

    # 住所文字列の正規化
    assert normalize_for_pydams('東京都 府中市 清水が丘１丁目８−３ 京王リトナード東府中1F') == '東京都 府中市 清水が丘１丁目８−３'
    assert normalize_for_pydams('府中市 清水が丘１丁目８−３ 京王リトナード東府中1F', pref_name='東京都') == '東京都 府中市 清水が丘１丁目８−３'
    assert normalize_for_pydams('東京都府中市清水が丘１丁目８−３京王リトナード東府中1F') == '東京都府中市清水が丘１丁目８−３'
    assert normalize_for_pydams('東京都府中市清水が丘一丁目八番地三号京王リトナード東府中1F') == '東京都府中市清水が丘一丁目八番地三号'
    assert normalize_for_pydams('東京都新宿区四谷1丁目無番地四ツ谷駅の中の自販機の前') == '東京都新宿区四谷1丁目無番地'

    try:
        normalize_for_pydams('東京都新宿区')
        assert False, '例外が発生しませんでした'
    except NormalizeError as e:
        pass

    print('success!!')
