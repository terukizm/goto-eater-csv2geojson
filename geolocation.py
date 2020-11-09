import re
from logzero import logger

class NormalizeError(Exception):
    pass

# 以下の正規表現に「無番地」を追加
# @see https://qiita.com/shouta-dev/items/b87efc19e105045881de
regex = r"([0-9０-９]+|[一二三四五六七八九十百千万]+)*(([0-9０-９]+|[一二三四五六七八九十百千万]+)|(丁目|丁|無番地|番地|番|号|-|‐|ー|−|の|東|西|南|北){1,2})*(([0-9０-９]+|[一二三四五六七八九十百千万]}+)|(丁目|丁|無番地|番地|番|号){1,2})"

def normalize(address: str):
    """
    pydamsが正しく結果を返せる形式に住所文字列を正規化
     ・番地以下、ビル名を含むと誤爆することがあるため除去
     ・スペースはあってもなくてもOK
     ・丁目、番地は漢数字・半角数字・全角数字どれでもOK
     ・xx-xx形式でも、xx番地xx丁目形式でもOK
    """
    if not address:
        return ''

    # 住所中のスペースは無しに倒す
    # address = re.sub(r"\s+", '', address)

    # 番地部分だけ抽出
    m = re.search(regex, address)
    if not m:
        raise NormalizeError();
    addr2 = m.group()
    addr1 = address.split(addr2)[0]

    return addr1 + addr2


if __name__ == "__main__":
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
