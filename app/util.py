import re

from pydams import DAMS
DAMS.init_dams()

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
     ・区切りスペースはあってもなくてもOK(どちらでもpydamsの結果には影響してないっぽい)
     ・丁目、番地は漢数字・半角数字・全角数字どれでもOK
     ・xx-xx形式でも、xx番地xx丁目形式でもOK
    """
    if not address:
        return ''

    # 番地部分だけ抽出
    m = re.search(regex, address)
    if not m:
        raise NormalizeError(f'address={address}');
    addr2 = m.group()
    addr1 = address.split(addr2)[0]

    return addr1 + addr2

def geocode(address: str):
    """
    PyDAMSを利用
    return (lat, lng, debuginfo=(score, tail, name))
    """
    try:
        geocoded = DAMS.geocode_simplify(address)
        lat = round(geocoded['candidates'][0]['y'], 6)  # 国土地理院地図に合わせて6桁とした
        lng = round(geocoded['candidates'][0]['x'], 6)
        score = geocoded['score']
        name = geocoded['candidates'][0]['name']    # ジオコーディングに寄与する住所
        tail = geocoded['tail']                     # ジオコーディングに寄与してない住所
        return (lat, lng, (score, name, tail))
    except Exception as e:
        raise GeocodeError(e)


if __name__ == "__main__":
    # pydams入ってないと動かないので(本当はmockしてあげたりするとよい)
    # $ docker-compose run csv2geojson python /app/util.py

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
