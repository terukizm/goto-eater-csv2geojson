# @see https://github.com/hottolink/pydams/blob/master/docker/Dockerfile
# @see https://github.com/morita-tenpei/docker-DAMS-geocoder/blob/master/Dockerfile

FROM python:3.6.10 as Builder
ENV DAMS_VERSION=4.3.4

RUN set -ex && \
  apt-get update && apt-get install -y git wget curl build-essential && \
  rm -rf /var/lib/apt/lists/*
RUN pip install -U pip && pip install Cython --no-cache-dir

# install DAMS
RUN cd /opt \
  && wget -q http://newspat.csis.u-tokyo.ac.jp/download/dams-${DAMS_VERSION}.tgz \
  && tar -xzvf dams-${DAMS_VERSION}.tgz \
  # apply patch(only 4.3.4(?))
  && wget -q https://raw.githubusercontent.com/hottolink/pydams/master/patch/dams-${DAMS_VERSION}.diff \
  && patch -d ./dams-${DAMS_VERSION} -p1 < ./dams-${DAMS_VERSION}.diff \
  # install DAMS
  && cd /opt/dams-${DAMS_VERSION} \
  && ./configure && make && make install && ldconfig && ldconfig -v | grep dams && make dic && make install-dic \
  # pydamsを使う上では、インストールされた/usr/local/lib/dams/* 以外のファイルは必須ではないので削除
  # DAMSの実行バイナリ(/opt/dams/src/dams)とか、住所データファイル(/opt/dams-4.3.4/data/dams.txt.gz)を利用したくて、
  # イメージサイズを気にしない場合は、残しておいてもよい
  && rm -rf /opt/dams /opt/dams-${DAMS_VERSION} /opt/dams-${DAMS_VERSION}.tgz

# install pydams
RUN git clone https://github.com/hottolink/pydams.git /opt/pydams && cd /opt/pydams && make all && rm -rf /opt/pydams

# pydamsの動作確認
CMD python -c 'from pydams import DAMS;from pydams.helpers import pretty_print; DAMS.init_dams(); pretty_print(DAMS.geocode("東京都千代田区千代田1-1"))'

# csv2geojson実行環境用のライブラリを追加
WORKDIR /tmp
COPY pyproject.toml poetry.lock ./
RUN pip install --no-cache-dir poetry && poetry config virtualenvs.create false && poetry install
