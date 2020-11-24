import re
import webbrowser
from pydams import DAMS
from logzero import logger

class NormalizeError(Exception):
    pass
class GeocodeError(Exception):
    pass

# 以下の正規表現に「無番地」を追加
# @see https://qiita.com/shouta-dev/items/b87efc19e105045881de
regex = r"([0-9０-９]+|[一二三四五六七八九十百千万]+)*(([0-9０-９]+|[一二三四五六七八九十百千万]+)|(丁目|丁|無番地|番地|番|号|-|‐|ー|−|の|東|西|南|北){1,2})*(([0-9０-９]+|[一二三四五六七八九十百千万]}+)|(丁目|丁|無番地|番地|番|号){1,2})"

def normalize(address: str):
    """
    pydamsが正しくジオコーディング結果を返せる形式に住所文字列を正規化
     ・番地以下、ビル名を含むと誤爆することがあるので除去
     ・区切りスペースはあってもなくてもOK
     ・丁目、番地は漢数字・半角数字・全角数字どれでもOK
     ・xx-xx形式でも、xx番地xx丁目形式でもOK
    """
    if not address:
        return ''

    # 住所中のスペースは無しに倒す -> とりあえずどちらでもpydamsの結果には影響してないっぽい
    # address = re.sub(r"\s+", '', address)

    # 番地部分だけ抽出
    m = re.search(regex, address)
    if not m:
        raise NormalizeError(f'address={address}');
    addr2 = m.group()
    addr1 = address.split(addr2)[0]

    return addr1 + addr2

def geocode(address: str):
    """
    return (lat, lng, debuginfo=(score, tail, name))
    """
    try:
        geocoded = DAMS.geocode_simplify(address)
        lat = round(geocoded['candidates'][0]['y'], 6)  # 国土地理院地図に合わせて6桁とした
        lng = round(geocoded['candidates'][0]['x'], 6)
        score = geocoded['score']
        name = geocoded['candidates'][0]['name']
        tail = geocoded['tail']  # ジオコーディングに寄与してない住所
        return (lat, lng, (score, name, tail))
    except Exception as e:
        raise GeocodeError(e)

def open_with_gsimap(lat, lng):
    """
    lat, lngを指定して地理院地図(電子国土Web)で開く
    """
    webbrowser.open(f"https://maps.gsi.go.jp/#18/{lat}/{lng}/&base=std&ls=std&disp=1&vs=c1j0h0k0l0u0t0z0r0s0m0f1")

def open_with_simple_geocode(address: str):
    """
    DAMSを提供しているCSISが提供しているシンプルジオコーディングサービスを使って、変換結果を確認する
    @see http://newspat.csis.u-tokyo.ac.jp/geocode/modules/geocode/index.php?content_id=4
    """
    # TODO: 下記相当のリクエストを投げる
    # $ curl -sS -X POST "http://newspat.csis.u-tokyo.ac.jp/cgi-bin/simple_geocode.cgi" -d "charset=UTF8" -d "addr=栃木県佐野市大橋町3229-7" -d "geosys=world" -d "series=ADDRESS" -d "submit=検索" | tidy -q -i -xml -utf8
    pass

def normalize_genre_by_code(genre_name: str):
    """
    ジャンル名を寄せる

    難しかったもの
    ・「旅館」「ホテル」「旅館・ホテル」あたり。旅館=和食、ホテル=洋食としても「旅館・ホテル」というジャンルで登録されてるやつもあるので死
    ・「そば(5:麺類)」と「焼きそば(10:その他)」「中華そば(4:中華ではない)」の関係性
    ・富山県は「焼き鳥・焼肉」っていうカテゴリがあり、焼き鳥(1:居酒屋系)と焼肉(7:系)とまたがるのでどうしたもんか

    東京(ぐ○なび)はきりがないので適当、ぶっちゃけどうでもいい
    """
    # 複数カテゴリ指定されている場合、最初のだけで判定
    genre_name = genre_name.split('|')[0]

    # 10. その他
    if not genre_name:
        return 10
    if re.search(r'その他|お好み焼|焼きそば|たこ焼|もんじゃ|イートイン|旅館|ホテル|飲食店', genre_name):
        # MEMO: 旅館・ホテルあたりは他に置くところがない。また「焼きそば」を「そば」より先にHitさせる必要がある
        return 10

    # 8: 'ファーストフード・ファミレス・食堂',
    if re.search('ハンバーガー|ファーストフード|ファストフード|ファミレス|レストラン|バイキング|' +
        '定食|食事処|食堂|フライドチキン|から揚げ|ザンギ|サンドイッチ|サンドウィッチ|牛丼|軽食|弁当', genre_name):
        # MEMO: "ハンバーガー"が"バー"と誤Hitするので先に判定
        return 8

    # 7: 'ステーキ・焼肉・ホルモン・すき焼き・しゃぶしゃぶ',
    if re.search(r'焼肉|ステーキ|鉄板|ホルモン|すき焼き|もつ焼|もつやき|しゃぶしゃぶ|ジンギスカン|牛たん|牛タン', genre_name):
        # 富山県の「焼き鳥・焼肉」というジャンルを焼肉側に倒すため
        return 7

    # 1: 居酒屋・バー・ダイニングバー・バル
    if re.search(r'居酒屋|バル|バー|酒場|ビヤホール|ビアホール|ビアレストラン|カクテル|ビール|ワイン|日本酒|ハイボール|'
        'やきとん|やきとり|焼鳥|焼き鳥|焼きとり|串揚|串カツ|串焼|炉端焼き|パブ|スナック|クラブ|呑み|宴会', genre_name):
        return 1
    # 2: 和食
    if re.search(r'和食|和風|日本料理|郷土料理|沖縄|九州|京料理|懐石|会席|割烹|料亭|小料理|天ぷら|刺身|' +
        'うなぎ|ふぐ|はも|うに|すっぽん|あなご|あんこう|川魚|魚料理|鶏料理|' +
        'とんかつ|かに料理|海鮮|おにぎり|お茶漬け|釜飯|おでん|鍋|すし|寿司|ひつまぶし', genre_name):
        # もうわかんなくなってきた...
        return 2
    # 3: 洋食・フレンチ・イタリアン,
    if re.search(r'洋食|欧風|オムライス|シチュー|フランス|フレンチ|イタリア|ドイツ|イギリス|スペイン|西洋|ヨーロッパ|' +
        'スパゲティ|パスタ|ピザ|ビストロ|アメリカ|ロシア|地中海|ハワイアン', genre_name):
        return 3
    # 5: 'うどん・そば・ラーメン・餃子・丼',
    if re.search(r'ラーメン|らーめん|つけめん|そば|蕎麦|うどん|ちゃんぽん|麺|中華そば|餃子|丼', genre_name):
        # MEMO: "中華そば"という文字列を"中華"より先にmatchさせないといけない
        # さらに"焼きそば"という文字列は後でmatchせないといけない
        # 餃子とか丼とかはジャンルとしてここなのかもわからん、何もかもわからない
        return 5
    # 4: 中華,
    if re.search(r'中華|中国|台湾|四川|広東|上海|点心', genre_name):
        return 4
    # 6: カレー・アジア・エスニック・各国料理,
    if re.search(r'アジア|エスニック|韓国|無国籍|多国籍|南米|各国|創作|インド|カレー|メキシコ|ブラジル|アフリカ|' +
        'ベトナム|トルコ|タイ料理|フォー', genre_name):
        return 6
    # 9: カフェ・スイーツ,
    if re.search(r'カフェ|Cafe|パーラー|スイーツ|コーヒー|クレープ|パンケーキ|喫茶|甘味|珈琲|紅茶|茶房|' +
        'パフェ|チョコレート|アイスクリーム|菓子|デザート|ケーキ', genre_name):
        return 9

    # それ以外はその他に全部寄せる
    # (例外投げてもよいが、東京都とかしれっと新ジャンル追加されるので...)
    logger.warning(f'未知のジャンル名: 「{genre_name}」')
    return 10


if __name__ == "__main__":
    # ジャンルの正規化
    assert normalize_genre_by_code('') == 10        # TODO: 定数化
    assert normalize_genre_by_code(None) == 10
    assert normalize_genre_by_code('焼きそば専門店') == 10
    assert normalize_genre_by_code('ハンバーガーヒル') == 8
    assert normalize_genre_by_code('フレンチ食堂') == 8
    assert normalize_genre_by_code('創作居酒屋カフェ') == 1
    assert normalize_genre_by_code('謎のジャンル') == 10

    # 住所文字列の正規化
    assert normalize('東京都 府中市 清水が丘１丁目８−３ 京王リトナード東府中1F') == '東京都 府中市 清水が丘１丁目８−３'
    assert normalize('東京都府中市清水が丘１丁目８−３京王リトナード東府中1F') == '東京都府中市清水が丘１丁目８−３'
    assert normalize('東京都府中市清水が丘一丁目八番地三号京王リトナード東府中1F') == '東京都府中市清水が丘一丁目八番地三号'
    assert normalize('東京都新宿区四谷1丁目無番地四ツ谷駅の中の自販機の前') == '東京都新宿区四谷1丁目無番地'
    try:
        normalize('東京都新宿区')
        assert False, '例外が発生しませんでした'
    except NormalizeError as e:
        pass
