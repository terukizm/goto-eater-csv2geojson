# 静岡県には赤券、青券があるが、「どちらか一方にだけ」データが存在するものが
# どれだけあるかを確認するための、雑スクリプト。

# usage:
# $ poetry run python working/shizuoka_blue_or_red.py

# 寄せ対象として電話番号を用いたが、それでも1000件近くある。
# 差分には、明らかに電話番号の入力ミスとか片方だけしか電話番号が入力されてないといったものがあるので、おわりだよ

import pandas as pd
import pathlib

# pd.set_option("display.max_colwidth", 200)
# pd.set_option('display.max_rows', 1000)

input_csv_dir = pathlib.Path.cwd() / 'data' / 'input' / 'csvs'
red_df = pd.read_csv(input_csv_dir / 'shizuoka.csv', encoding="utf-8", dtype=str).fillna('')
blue_df = pd.read_csv(input_csv_dir / 'shizuoka_blue.csv', encoding="utf-8", dtype=str).fillna('')

join_key = 'tel'            # 突き合わせキーとしては、かろうじて一番マシ(それでも1000件近く差分が出る)
# join_key = 'shop_name'    # (有)とか全角半角とか記入ブレが多数あってダメ
# join_key = 'address'      # スペースの有無とか全角半角とか記入ブレが多数あってダメ

merged = red_df.merge(blue_df, how='left', on=join_key, indicator=True)
red_only = merged[merged['_merge']=='left_only']
merged = blue_df.merge(red_df, how='left', on=join_key, indicator=True)
blue_only = merged[merged['_merge']=='left_only']

both = blue_df.merge(red_df, how='inner', on=join_key)
both = both['tel'].unique()

print('赤券={}, 青券={} (red_only={}, blue_only={})'.format(len(red_df), len(blue_df), len(red_only), len(blue_only)))
# print('BOTH={}'.format(len(both)))  # 電話番号がないやつとかがあるから計算が合わない、参考程度に

# columns = ['shop_name_x', 'address', 'tel_x']
# columns = ['shop_name_x', 'address_x', 'tel']

# print('赤券のみ(https://gotoeat.s-reserve.com/)')
# print(red_only[columns])
# print('青券のみ(https://gotoeat-shizuoka.com/shop/)')
# print(blue_only[columns])
