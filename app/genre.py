import re
from logzero import logger

GENRE_居酒屋 = 1
GENRE_和食 = 2
GENRE_洋食 = 3
GENRE_中華 = 4
GENRE_麺類 = 5
GENRE_各国料理 = 6
GENRE_焼肉 = 7
GENRE_ファミレス = 8
GENRE_カフェ = 9
GENRE_その他 = 10

def classify(genre_name: str):
    """
    各都道府県で好き勝手に設定されているジャンル名を寄せる

    難しかったもの
    ・「旅館」「ホテル」「旅館・ホテル」旅館=和食、ホテル=洋食と仮定しても「旅館・ホテル」というジャンルで登録されてるやつもあるので死
    ・「そば(5:麺類)」と「焼きそば(10:その他)」「中華そば(4:中華ではなく5:麺類)」の関係性
    ・富山県は「焼き鳥・焼肉」っていうカテゴリがあり、焼き鳥(1:居酒屋系)と焼肉(7:系)とまたがる
    ・東京(ぐる○び)のよくわかんないジャンル名全般→きりがないので適当に切り上げた
    """
    # ジャンル分けをそもそも採用していない自治体もある
    if not genre_name:
        return GENRE_その他

    # 複数カテゴリ指定されている場合、最初のものだけで判定
    genre_name = genre_name.split('|')[0]

    # 10. その他
    if re.search('その他|お好み焼|焼きそば|粉物|たこ焼|もんじゃ|イートイン|旅館|ホテル|飲食店', genre_name):
        # MEMO: 「焼きそば」を「そば」より先にHitさせる必要がある
        # 旅館・ホテルあたりはレストラン寄りなのかもしれない…　何もわからん…
        return GENRE_その他

    # 8: ファーストフード・ファミレス・食堂
    if re.search('ハンバーガー|ファーストフード|ファストフード|ファミレス|レストラン|バイキング|ドライブイン' +
        '|定食|食事処|食堂|フライドチキン|から揚げ|ザンギ|サンドイッチ|サンドウィッチ|牛丼|軽食|弁当', genre_name):
        # MEMO: "ハンバーガー"が"バー"と誤Hitするので判定順を前に
        return GENRE_ファミレス

    # 7: ステーキ・鉄板焼・焼肉・ホルモン,
    if re.search(r'焼肉|ステーキ|鉄板|ホルモン|もつ焼|もつやき|ジンギスカン|牛たん|牛タン', genre_name):
        # 富山県の「焼き鳥・焼肉」というジャンルを、焼肉側に倒すためにこの判定順
        return GENRE_焼肉

    # 1: 居酒屋・バー・ダイニングバー・バル
    if re.search('居酒屋|バル|バー|BAR|酒場|ビヤホール|ビアホール|ビアガーデン|ビアレストラン|屋形船' +
        '|カクテル|ビール|ワイン|日本酒|ハイボール|呑み|宴会' +
        '|やきとん|やきとり|焼鳥|焼き鳥|焼きとり|串揚|串カツ|串焼|炉端焼き|牡蠣小屋|パブ|スナック|クラブ|ラウンジ', genre_name):
        return GENRE_居酒屋

    # 2: 和食
    if re.search('和食|和風|日本料理|郷土料理|沖縄|九州|京料理|懐石|会席|割烹|料亭|小料理|天ぷら|刺身' +
        '|うなぎ|ふぐ|はも|うに|すっぽん|あなご|あんこう|すき焼き|しゃぶしゃぶ|川魚|魚料理|鶏料理' +
        '|とんかつ|かに料理|海鮮|おにぎり|お茶漬け|釜飯|おでん|鍋|ちゃんこ|水炊き|すし|寿司|ひつまぶし', genre_name):
        # もうわかんなくなってきた...
        # 「しゃぶしゃぶ、すき焼き」を「肉料理」として焼肉と並列で扱っている都道府県もあるが、
        # ここでは愛媛に準拠して「鍋料理」と一緒のカテゴリにしてる
        return GENRE_和食

    # 3: 洋食・フレンチ・イタリアン,
    if re.search(r'洋食|欧風|オムライス|シチュー|フランス|フレンチ|イタリア|ドイツ|イギリス|スペイン|西洋|ヨーロッパ' +
        '|スパゲティ|パスタ|ピザ|ピッツァ|ビストロ|アメリカ|ロシア|地中海|ハワイアン', genre_name):
        return GENRE_洋食

    # 5: うどん・そば・ラーメン・餃子・丼
    if re.search(r'ラーメン|らーめん|つけめん|そば|蕎麦|うどん|ちゃんぽん|麺|麵|中華そば|餃子|丼', genre_name):
        # MEMO: "中華そば"という文字列を"中華"より先にmatchさせないといけない
        # さらに"焼きそば"という文字列はここではmatchせないとようにしないといけない
        # 餃子とか丼とかはジャンルとしてここなのかもわからん、何もかもわからない
        return GENRE_麺類

    # 4: 中華
    if re.search(r'中華|中国|台湾|四川|広東|上海|点心|飲茶', genre_name):
        return GENRE_中華

    # 6: カレー・アジア・エスニック・各国料理
    if re.search(r'アジア|エスニック|韓国|朝鮮|無国籍|多国籍|南米|各国|インド|カレー|メキシコ|ブラジル|アフリカ' +
        '|ベトナム|トルコ|タイ料理|フォー|ネパール|創作', genre_name):
        # MEMO: 山形県は「居酒屋・創作料理」というジャンルがあるが、居酒屋に寄せた
        return GENRE_各国料理

    # 9: カフェ・スイーツ
    if re.search(r'カフェ|Cafe|パーラー|スイーツ|コーヒー|クレープ|パンケーキ|喫茶|甘味|珈琲|紅茶|茶房' +
        '|パフェ|チョコレート|アイスクリーム|菓子|デザート|ケーキ|ドーナツ', genre_name):
        return GENRE_カフェ

    # それ以外は 10:その他 に全部寄せる
    # (例外投げてもよいが、東京都(ぐるな○)はしれっと新ジャンル追加されるので...)
    logger.warning(f'未知のジャンル名: 「{genre_name}」')
    return GENRE_その他


if __name__ == "__main__":
    # ジャンルの正規化
    assert classify('') == GENRE_その他
    assert classify(None) == GENRE_その他
    assert classify('ハンバーガーヒル') == GENRE_ファミレス # 「ハンバーガー」に当たる
    assert classify('焼きそば専門店') == GENRE_その他    # 「焼きそば」に当たる
    assert classify('フレンチ食堂') == GENRE_ファミレス  # 先に「食堂」に当たる
    assert classify('創作居酒屋カフェ') == GENRE_居酒屋  # 先に「居酒屋」に当たる
    assert classify('謎のジャンル') == GENRE_その他

    # 「鍋」に当たるため。実際にはこういうジャンルの店はなかったが、こういう誤判定がありうるので注意が必要(「バー」と「ハン'バー'ガー」とか)
    assert classify('火鍋専門店') == GENRE_和食