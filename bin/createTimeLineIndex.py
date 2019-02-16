# -*- coding: utf-8 -*-

"""
ESにインデックス(再)作成する

Author : Sugimoto
"""

# ES
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

import logging
import os
import configparser
import zc.lockfile
import json


def main():
    """
    メイン処理
    
    Parameters
    -------
    なし
    
    Returns
    -------
    なし
    
    """
    
    pwd_path = os.getcwd() # カレントパス取得
    basename = __file__ # ファイル名
    filename , ext = os.path.splitext(basename) #ファイル名と拡張子を分離
    
    # Tweet取得スクリプトの設定ファイルから取ってくる
    conf_path = pwd_path + "/../conf/updateESfromTimeLine.conf"
    
    json_path = pwd_path + "/../conf/" + filename + ".json"
    log_path = pwd_path + "/../log/" + filename + ".log"
    loc_path = pwd_path + "/../log/" + filename + ".loc"
    
    # デバッグレベル取得
    common_config = getConfig("common", conf_path)
    
    # ロックファイル生成
    lock = zc.lockfile.LockFile(loc_path)
    
    # ログレベル (デフォINFO, DEBUGフラグ有のみDEBUGレべル)
    logging.basicConfig(level=logging.INFO)
    if common_config.get("DEBUG") == 1:
        logging.basicConfig(level=logging.DEBUG)

    # ロガー
    logger = logging.getLogger(filename)
    
    # ログのファイル出力先を設定
    fh = logging.FileHandler(log_path)
    logger.addHandler(fh)
    
    # 各種設定値を格納
    params = {
        "conf_path": conf_path,
        "json_path": json_path,
        "logger" : logger
    }
    
    result = createIndex(params)
    
    # ロック解除
    lock.close()
    # ロックファイル削除
    os.remove(loc_path)
    
    return

def getConfig (section, conf):
    """
    設定ファイルから任意セクションの設定値を取得する
    
    Parameters
    -------
    section : string
        取得したいセクション
    conf : string
        confファイルのパス(フルパス)
    
    Returns
    -------
    config[section] : dict
        指定セクションの設定値からなるdict
        キーはconfと同キー
    """
    
    # インスタンス生成
    config = configparser.ConfigParser()
    
    # config読み出し
    config.read(conf)
    
    result = {}
    result = config[section] # 特定セクションのconfigのみ取得
    
    return result

def createIndex (params):
    """
    ElasticsearchへIndexを作成する
    
    Parameters
    -------
    params : dict
        各種設定値が格納されたdict
    
    Returns
    -------
    なし
    """

    conf_path = params.get("conf_path")
    json_path = params.get("json_path")
    logger = params.get("logger")
    
    # config取得し、各種キーをセット
    config = getConfig("elasticsearch", conf_path)
    host = config.get("host")
    access_key = config.get("access_key")
    access_key_secret = config.get("access_key_secret")
    region = config.get("region")
    index_name = config.get("index")
    
    try:
        # AWS認証セット
        aws_auth = AWS4Auth(access_key, access_key_secret, region , 'es')
        
        # ES接続
        es = Elasticsearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=aws_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        
        # インデックスがすでに存在したら削除
        if es.indices.exists(index=index_name):
            logger.info("delete index")
            logger.info(es.indices.delete(index=index_name))
        
        # インデックス作成クエリJSON読み込む
        with open(json_path) as json_file:
            index_json = json.load(json_file)
    
        logger.info("create index")
        logger.info(es.indices.create(index=index_name, body=index_json))
        
    except Exception as e:
        #エラー出力
        logger.warning(e)
    
    return 0

# 処理開始
main ()

exit
    



