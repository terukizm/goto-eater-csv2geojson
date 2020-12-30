goto-eater-csv2json
===

# 概要

[goto-eater-crawler](https://github.com/terukizm/goto-eater-crawler)で取得、もしくは手作りしたCSVから、GeoJSON形式に変換

* ジオコーディングにより住所からlatlngを取得
  * [pydams](https://github.com/hottolink/pydams)を利用
* 重複チェック
  * 元データの入力不具合、crawlerの実装ミス、公式サイトの検索機能の不具合などの理由により、重複レコードが存在することがあるので
    * 住所と店名が完全一致している場合のみ重複しているとみなし、warningを出して重複レコードは削除
* 郵便番号によるチェック(ログ出力のみ)
  * 入力された住所形式によってはジオコーディングで全然関係のない住所が引かれてしまうことがあり、郵便番号(任意項目)が存在する場合はそれを使ってチェック
    * 「郵便番号自体が間違って入力されている場合」があり、その場合はどうしようもないのが難点…
* 住所の正規化
  * 住所にビル名が含まれると、そこにある文字列に引きずられてpydams(DAMS)が正しく結果を返してくれないことがあるので
* ジャンル名の正規化
  * 各都道府県により独自のジャンル分けがされているので、埼玉県のジャンル分類を参考に再分類(10種類)
    * 自由入力による、自由すぎるジャンル分けに対応(例: 東京都、岩手県)
    * 複数ジャンル名がある場合、最初に出てきたものを利用
* [GeoJSON](https://geojson.org/)形式での出力

# Usage

事前に`./data/input/csvs/*.csv`を配置しておくこと。
poetryでも実行可能だが、pydamsを動かす上でDAMSのインストールが必要なため、docker-composeでの実行を推奨。

```
$ git clone https://github.com/terukizm/goto-eater-csv2json
$ cd goto-eater-csv2json/

$ cp -a ...
$ ls data/input/csvs/*.csv
data/input/csvs/gunma.csv
data/input/csvs/oita.csv
data/input/csvs/kagoshima.csv
data/input/csvs/tochigi.csv

$ docker-compose build
(略)

$ docker-compose run csv2geojson python main.py
  or
$ docker-compose run csv2geojson python main.py --target tochigi,oita,gunma
```
