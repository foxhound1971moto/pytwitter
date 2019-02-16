# -*- coding: utf-8 -*-

"""
タイムライン上のツイ廃を判定

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
    
    pwd_path = os.path.dirname(os.path.abspath(__file__)) #パス取得
    basename = os.path.basename(__file__) # ファイル名
    filename , ext = os.path.splitext(basename) #ファイル名と拡張子を分離
    
    # Tweet取得スクリプトの設定ファイルから取ってくる
    conf_path = pwd_path + "/../conf/" + filename + ".conf"
    
    json_path = pwd_path + "/../conf/" + filename + ".json"
    log_path = pwd_path + "/../log/" + filename + ".log"
    loc_path = pwd_path + "/../log/" + filename + ".loc"
    
    # デバッグレベル取得
    common_config = getConfig("common", conf_path)
    
    # ロックファイル生成
    lock = zc.lockfile.LockFile(loc_path)
    
    # ロガー
    logger = logging.getLogger(filename)

    # ログファイルのフォーマット
    fmt = "%(asctime)s %(levelname)s %(name)s :%(message)s"

    # ログレベル (デフォINFO, DEBUGフラグ有のみDEBUGレべル)
    logging.basicConfig(filename=log_path, level=logging.INFO, format=fmt)
    if common_config.get("DEBUG") == 1:
        logging.basicConfig(filename=log_path, level=logging.DEBUG, format=fmt)
        
    # 各種設定値を格納
    params = {
        "conf_path": conf_path,
        "json_path": json_path,
        "logger" : logger
    }
    
    # ESを検索する(検索範囲の変更をクエリ(jsonファイル)で変更すること)
    es_result= searchFromEs(params)
    
    # ES検索結果のユーザごとのツイート数から廃人レベルを判定する
    result = judgeHaijin(params, es_result)
    
    # あとは煮るなり焼くなりお好きにどうぞ
    print(result)
    
    
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

def searchFromEs(params):
    """
    Elasticsearchを指定のクエリで検索する
    
    Parameters
    -------
    params : dict
        各種設定値が格納されたdict
    
    Returns
    -------
    results : dict
        ESから返却された検索結果
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
    
    result = {}
    
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
        
        # ES検索クエリのJSONを読み込む
        with open(json_path) as json_file:
            body = json.load(json_file)
    
        logger.info("start search")
        result = es.search(index=index_name, body=body)
        logger.info("finish search")
        
        logger.debug(result)
        
    except Exception as e:
        #エラー出力
        logger.warning(e)
        print(e)
    
    return result
    
def judgeHaijin(params, es_result):
    """
    Elasticsearchに格納したタイムラインからツイ廃レベルを判定する
    
    Parameters
    -------
    params : dict
        各種設定値が格納されたdict
    es_result : dict
        ES検索結果
    
    Returns
    -------
    results : list
        ユーザごとのレベル
    """

    conf_path = params.get("conf_path")
    json_path = params.get("json_path")
    logger = params.get("logger")
    
    # config取得し、各種キーをセット
    config = getConfig("common", conf_path)
    # 廃人レベルをロード
    haijin_level = json.loads(config.get("HAIJIN_LEVEL"))
    
    # カウント数が格納されている箇所を抜き出す
    user_counts = es_result["aggregations"]["user_name_aggs"]["buckets"]
    
    results = []
    if len(user_counts) == 0:
        logger.info("no tweets 1 hour ago")
    else:
        user_result={}
        for user in user_counts:
            tweet_count = user["doc_count"]
            
            # 廃人レベルを判定する
            tweet_user = user["key"]
            for key, value in haijin_level.items():
                if value["min"] <= tweet_count and tweet_count <= value["max"]:
                    user_result={
                        "user_name": tweet_user,
                        "tweet_count": tweet_count,
                        "level": key
                    }
                    break;
                else:
                    continue
            results.append(user_result)
            
    logger.debug(results)
    return results

# 処理開始
main ()

exit
    



