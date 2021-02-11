import re
from functools import lru_cache

import pandas as pd
import posuto
import w3lib.html
from logzero import logger
from pydams import DAMS
from validator_collection import checkers

from .exceptions import GeocodeError, NormalizeError, ValidationWarning

DAMS.init_dams()


@lru_cache(maxsize=None)
def cached_posuto_pref(zip_code: str):
    """ posutoが内部的にsqlite3を使っており、バッチ用途で使うと結構遅くなるので、簡易キャッシュさせている """
    return posuto.get(zip_code).prefecture


def validate(row: pd.Series):
    """
    入力データに対する、お気持ち程度のバリデーションチェック
    """
    official_page = row["official_page"]
    if official_page and checkers.is_url(official_page) == False:
        raise ValidationWarning("公式URL(officical_page)が不正です")
    detail_page = row["detail_page"]
    if detail_page and checkers.is_url(detail_page) == False:
        raise ValidationWarning("詳細ページ(detail_page)のURLが不正です")

    # 郵便番号、電話番号の書式のバリデーション(厳密ではない)
    remove_char_regex = r"[ -‐－‑ー−‒–—―ｰ　]"  # (区切り文字適当)
    tel = re.sub(remove_char_regex, "", row["tel"])
    if tel and not re.match(r"^0\d{9,10}$", tel):
        raise ValidationWarning("電話番号(tel)の書式が不正です")  # 0始まりの半角数字9〜10桁
    zip_code = re.sub(remove_char_regex, "", row["zip_code"])
    if zip_code and not re.match(r"\d{7}$", zip_code):
        raise ValidationWarning("郵便番号(zip_code)の書式が不正です")  # 半角数字7桁

    # HTMLタグが含まれてほしくないやつに含まれている
    for target in [
        "shop_name",
        "address",
        "official_page",
        "detail_page",
        "opening_hours",
        "closing_day",
        "area_name",
    ]:
        text = row.get(target)
        if not text:
            continue
        if len(text) != len(w3lib.html.remove_tags(text)):
            raise ValidationWarning(f"{target}にHTMLタグが含まれています")

    # 郵便番号でのジオコーディング結果に対する正当性チェック
    try:
        zip_code = row["zip_code"]
        if not zip_code:
            return
        pref = cached_posuto_pref(zip_code)
    except KeyError:
        # MEMO: posutoのデータには存在しない(特殊な)郵便番号が指定されている場合がある
        # いわゆる「大口事業所個別番号」というやつで、そういうのはどうしようもないのでバリデーション成功とする
        logger.info(f"不明な郵便番号です (「大口事業所個別番号」かも？) : zip code={zip_code}")
        return
    except Exception as e:
        # MEMO: その他特殊すぎる郵便番号などでposuto内部でエラーが起きた場合
        logger.warning(e, stack_info=True)
        logger.warning(f"unknown posuto error, zip code={zip_code}")
        raise ValidationWarning(f"posutoでエラーになる郵便番号です(内部処理エラー)")

    norm_addr = row.get("normalized_address")
    if norm_addr and not norm_addr.startswith(pref):
        raise ValidationWarning(f"郵便番号から求められた都道府県は {pref} ですが、ジオコーディングされた住所は {norm_addr} です")

    # MEMO: 簡易的なものであり、「愛知県名古屋市xxxx」を「名古屋xxxx」と誤入力されたことで、
    # 千葉県成田市、新潟県佐渡市にある地名の「名古屋」のように、明らかに誤ったジオコーディング結果になっている場合に
    # 検出できる、という程度のもの。posutoを用いているが、結果は県名までしか利用していない。
    # (posuto.cityなどの値も使えることは使えるが、pydamsが返す住所形式とposutoが返す結果の住所形式が異なることがあり
    #  例: pydams = "栃木県/塩谷郡/高根沢町",  posuto = "栃木県/高根沢町"
    # どちらも住所形式としては正しく、同一の住所を指すが、こういった郡とか字(大字)の正規化まで考えていくと頭がおかしくなってくるので)

    # MEMO: 郵便番号チェックに失敗した場合、以下のようなケースがあり、一概にエラーであると処理できないため、Warningとしている
    # 1. ジオコーディングに実は失敗している(GeocodeErrorにはならないが、誤った結果を返している。理由は入力値が悪い、もしくは正規化が不十分)
    #   => このケースを想定して実装したが、件数的にはほぼない。それより以下の2.3.とひとつずつ確認してかないと区別できないのが辛い
    # 2. 郵便番号がそもそも間違っている (公式サイトの元データが悪い)
    #   => どうしようもない。住所の方が正しければ(まあ)問題はない。こっちの方が明らかに多い
    # 3. 郵便番号がそもそも「複数の都道府県にまたがる」郵便番号である (@see https://api.nipponsoft.co.jp/zipcode/498-0000 )
    #   => 「郵便番号の仕様」であり、どうしようもない


# 以下の正規表現に「無番地」を追加
# @see https://qiita.com/shouta-dev/items/b87efc19e105045881de
regex = r"([0-9０-９]+|[一二三四五六七八九十百千万]+)*(([0-9０-９]+|[一二三四五六七八九十百千万]+)|(丁目|丁|無番地|番地|番|号|-|の|東|西|南|北){1,2})*(([0-9０-９]+|[一二三四五六七八九十百千万]}+)|(丁目|丁|無番地|番地|番|号){1,2})"


def normalize_for_pydams(address: str, pref_name: str):
    """
    pydamsが正しくジオコーディング結果を返せる形式に住所文字列を正規化
     ・番地以下、ビル名を含むと誤爆することがあるので除去
     ・区切りスペースはあってもなくてもOK(どちらでもpydamsの結果には影響しないっぽい)
     ・丁目、番地は漢数字・半角数字・全角数字どれでもOK
     ・xx-yy形式でも、xx番地yy丁目形式でもOK
    """
    if not address:
        return ""

    # 番地部分だけ抽出
    address = re.sub("[-‐－‑ー−‒–—―ｰ]", "-", address)
    m = re.search(regex, address)
    if not m:
        raise NormalizeError(f"住所の正規化に失敗しました。 address={address}")
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
        raise GeocodeError("ジオコーディングの結果がありせんでした。(内部エラー)")

    lat = round(geocoded["candidates"][0]["y"], 6)  # 国土地理院地図に合わせて6桁とした
    lng = round(geocoded["candidates"][0]["x"], 6)

    if lat == 0 and lng == 0:
        raise GeocodeError("ジオコーディング結果がlat=0, lng=0(Null島)を示しています。")

    score = geocoded["score"]  # 1〜5
    name = geocoded["candidates"][0]["name"]  # ジオコーディングに寄与する住所
    tail = geocoded["tail"]  # ジオコーディングに寄与してない住所

    return (lat, lng, (score, name, tail))


def pref_name_ja_from_roman(pref_name: str):
    """
    例: pref_name_ja_from_roman(pref_name='tochigi') -> '栃木県'
    """
    # FIXME: 静岡(青券)のクソ対応
    if pref_name == "shizuoka_blue":
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


if __name__ == "__main__":
    # pydams入ってないと動かないので(本当はmockしてあげたりするとよい)
    # $ docker-compose run csv2geojson python -m csv2geojson.util

    # 住所文字列の正規化
    assert normalize_for_pydams("東京都 府中市 清水が丘１丁目８−３ 京王リトナード東府中1F", pref_name="tokyo") == "東京都 府中市 清水が丘１丁目８-３"
    assert normalize_for_pydams("府中市 清水が丘１丁目８−３ 京王リトナード東府中1F", pref_name="tokyo") == "東京都府中市 清水が丘１丁目８-３"
    assert normalize_for_pydams("東京都府中市清水が丘１丁目８−３京王リトナード東府中1F", pref_name="tokyo") == "東京都府中市清水が丘１丁目８-３"
    assert normalize_for_pydams("東京都府中市清水が丘一丁目八番地三号京王リトナード東府中1F", pref_name="tokyo") == "東京都府中市清水が丘一丁目八番地三号"
    assert normalize_for_pydams("東京都新宿区四谷1丁目無番地四ツ谷駅の中の自販機の前", pref_name="tokyo") == "東京都新宿区四谷1丁目無番地"

    try:
        normalize_for_pydams("東京都新宿区", pref_name="tokyo")
        assert False, "例外が発生しませんでした"
    except NormalizeError:
        pass

    print("success!!")
