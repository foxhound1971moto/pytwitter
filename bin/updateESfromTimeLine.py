# -*- coding: utf-8 -*-

"""
TwitterのタイムラインをElasticSearchに追加/更新を実施する

Author : Sugimoto
"""

# ES
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# Twitter
import tweepy

import logging
import os
import configparser
import pprint
import zc.lockfile

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
    
    conf_path = pwd_path + "/../conf/" + filename + ".conf"
    line_path = pwd_path + "/../var/" + filename + ".line"
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
        "line_path": line_path,
        "logger" : logger
    }
    
    tweets = getTweet (params)
    result = updateElasticSearchbyTweets(tweets, params)
    
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
    
def setConfig (section, key, value, conf):

    """
    設定ファイルの任意セクションへ値を設定する
    
    Parameters
    -------
    section : string
        設定値のセクション
    section : string
        設定値のキー
    value : string
        設定値
    conf : string
        confファイルのパス(フルパス)
    
    Returns
    -------
    なし
    
    """
    
    # インスタンス生成
    config = configparser.ConfigParser()
    # conf読み込み
    config.read(conf)
    
    # confファイル設定変更
    config.set(section, key, value)
    
    # ファイルに書き出す
    with open(conf, "w", encoding='utf8') as f:
        config.write(f)
        
    return 
    
def getTweet (params):
    """
    Tweet取得
    
    Parameters
    -------
    params : dict
        各種設定値が格納されたdict
    
    Returns
    -------
    tweets : list
        tweet(dict)が入ったlist
    """
    
    conf_path = params.get("conf_path")
    line_path = params.get("line_path")
    logger = params.get("logger")
    
    # config取得し、各種キーをセット
    tweetConfig = getConfig("twitter", conf_path)
    lineConfig = getConfig("last_updated", line_path)
    
    max_count = tweetConfig.get("max_count", conf_path)
    api_key = tweetConfig.get("api_key", conf_path)
    api_secret = tweetConfig.get("api_secret", conf_path)
    access_token = tweetConfig.get("access_token", conf_path)
    access_token_secret = tweetConfig.get("access_token_secret", conf_path)
    
    last_max_id = lineConfig.get("last_max_id")
    if len(last_max_id) <= 0:
        last_max_id = None
    last_max_id = None

    tweets = []  #初期化
    try:
        logger.info("start collect tweet")
        
        # api_key, secretセット
        auth = tweepy.OAuthHandler(api_key, api_secret)
        # tokenセット
        auth.set_access_token(access_token, access_token_secret)
        
        # ハンドラ
        api = tweepy.API(auth)
        
        if last_max_id != None:
            max_id = last_max_id
            results = api.home_timeline(since_id=last_max_id, count=max_count) #前回のID以降のタイムライン取得
        else:
            results = api.home_timeline(count=max_count) #タイムライン取得
            max_id = 0
        
        # Tweetを表示
        for result in results:
            tweet = {} #初期化
            
            # max_id更新
            if max_id < result.id:
                max_id = result.id 
            
           # pp.pprint(result)
            print (result.user.name)
            print (result.text)
            print (result.created_at)
            print (result.id)
            print ("------\r\n")
            
            # 必要な情報のみに絞る
            tweet = {
                "id":               result.id,
                "user_name":        result.user.name,
                "text":             result.text, 
                "coordinates" :     result.coordinates,
                "favorite_count" :  result.favorite_count,
                "retweet_count"  :  result.retweet_count,
                "is_quote_status" : result.is_quote_status, #リツイートかどうか(boolean)
                "in_reply_to_screen_name" : result.in_reply_to_screen_name,
                "created_at" :      result.created_at
            }
            
            #lishに追加
            tweets.append(tweet)
            
        # last_max_id更新
        setConfig("last_updated", "last_max_id", str(max_id), line_path)
        
        logger.debug(tweets)
        
        num = len(tweets)
        logger.info("get " + str(len(tweets)) + " tweets" )
        logger.info("finish collect tweets")
        
    except tweepy.TweepError as e:
        # エラー出力
        print(e)
        logging.warning(e)
    
    return tweets
    
def updateElasticSearchbyTweets (tweets, params):
    """
    ElasticsearchへTweetsを更新
    
    Parameters
    -------
    tweets : list
        tweet情報が入ったdictのlist
    params : dict
        各種設定値が格納されたdict
    
    Returns
    -------
    なし
    """

    conf_path = params.get("conf_path")
    line_path = params.get("line_path")
    logger = params.get("logger")
    
    # config取得し、各種キーをセット
    config = getConfig("elasticsearch", conf_path)
    host = config.get("host")
    access_key = config.get("access_key")
    access_key_secret = config.get("access_key_secret")
    region = config.get("region")
    print (access_key)
    print (access_key_secret)
    print (host)
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
    
    # ES 状態取得
    print(es.info())
    
    return 0

# 処理開始
main ()

exit
    



