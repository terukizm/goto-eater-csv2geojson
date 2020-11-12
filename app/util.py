import re
import webbrowser
from pydams import DAMS

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
    geocoded = DAMS.geocode_simplify(address)
    lat = round(geocoded['candidates'][0]['y'], 6)  # 国土地理院地図に合わせて6桁とした
    lng = round(geocoded['candidates'][0]['x'], 6)
    score = geocoded['score']
    name = geocoded['candidates'][0]['name']
    tail = geocoded['tail']  # ジオコーディングに寄与してない住所
    # TODO: Error処理(raise GeocodeError)
    return (lat, lng, (score, name, tail))

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

if __name__ == "__main__":
    # 住所文字列の正規化
    assert normalize('堺市北区中百舌鳥町') == '東京都 府中市 清水が丘１丁目８−３'
    assert normalize('東京都 府中市 清水が丘１丁目８−３ 京王リトナード東府中1F') == '東京都 府中市 清水が丘１丁目８−３'
    assert normalize('東京都府中市清水が丘１丁目８−３京王リトナード東府中1F') == '東京都府中市清水が丘１丁目８−３'
    assert normalize('東京都府中市清水が丘一丁目八番地三号京王リトナード東府中1F') == '東京都府中市清水が丘一丁目八番地三号'
    assert normalize('東京都新宿区四谷1丁目無番地四ツ谷駅の中の自販機の前') == '東京都新宿区四谷1丁目無番地'
    try:
        normalize('東京都新宿区')
        assert False, '例外が発生しませんでした'
    except NormalizeError as e:
        pass
