# -*- coding: utf-8 -*-

"""
TwitterのタイムラインをElasticSearchに追加/更新を実施する
"""

# ES
from elasticsearch import Elasticsearch

# Twitter
import tweepy


import logging
import os
import configparser
import pprint

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
    tweets = getTweet ()
    result = updateElasticSearchbyTweets(tweets)
    
    return 0
    
def getConfig (section):

    """
    設定ファイルから任意セクションの設定値を取得する
    
    Parameters
    -------
    section : string
        confファイルのセクション
    
    Returns
    -------
    config[section] : dict
        指定セクションの設定値からなるdict
        キーはconfと同キー
    
    """
    
    pwd_path = os.getcwd() # カレントパス取得
    basename = __file__ # ファイル名
    filename , ext = os.path.splitext(basename) #ファイル名と拡張子を分離
    conf_path = pwd_path + "/../conf/" + filename + ".conf" #confのファイルパスをセット(スクリプトと同名)
    
    # インスタンス生成
    config = configparser.ConfigParser()
    
    # config読み出し
    config.read(conf_path)
    
    result = {}
    result = config[section] # 特定セクションのconfigのみ取得
    
    return result
    
def updateLastMaxId (max_id):

    """
    confファイルのlast_max_id更新
    
    Parameters
    -------
    Id : int
        前回取得したtweetの最大Id
    
    Returns
    -------
    なし
    
    """
    
    pwd_path = os.getcwd() # カレントパス取得
    basename = __file__ # ファイル名
    filename , ext = os.path.splitext(basename) #ファイル名と拡張子を分離
    conf_path = pwd_path + "/../var/" + filename + ".line" #confファイルパスをセット(スクリプトと同名)
    
    # インスタンス生成
    config = configparser.ConfigParser()
    # conf読み込み
    config.read(conf_path)
    # confファイル設定変更
    config.set("last_updated","last_max_id", str(max_id))
    # ファイルに書き出す（注意！現状だとコメントや改行を消してしまいます）
    with open(conf_path, "w", encoding='utf8') as f:
        config.write(f)
    return 0
    
    
def getLastMaxId ():

    """
    lastMaxId取得
    
    Parameters
    -------
    なし
    
    Returns
    -------
    
    
    """
    
    pwd_path = os.getcwd() # カレントパス取得
    basename = __file__ # ファイル名
    filename , ext = os.path.splitext(basename) #ファイル名と拡張子を分離
    conf_path = pwd_path + "/../var/" + filename + ".line" #confファイルパスをセット(スクリプトと同名)
    
    # インスタンス生成
    config = configparser.ConfigParser()
    # conf読み込み
    config.read(conf_path)
    
    result = {}
    result = config["last_updated"] # 特定セクションのconfigのみ取得
    
    return result

    
def getTweet ():
    """
    Tweet取得
    
    Parameters
    -------
    なし
    
    Returns
    -------
    なし
    
    """
    
    # config取得し、各種キーをセット
    config = getConfig("twitter")
    max_count = config.get("max_count")
    api_key = config.get("api_key")
    api_secret = config.get("api_secret")
    access_token = config.get("access_token")
    access_token_secret = config.get("access_token_secret")
     
    #例外処理
    try:
        logging.debug("start collect tweet")
        
        # api_key, secretセット
        auth = tweepy.OAuthHandler(api_key, api_secret)
        # tokenセット
        auth.set_access_token(access_token, access_token_secret)
        
        # ハンドラ
        api = tweepy.API(auth)
        
        configLastUpdated = getLastMaxId()
        last_max_id = int(configLastUpdated.get("last_max_id"))
    
        results = api.home_timeline(since_id=last_max_id ,count=max_count) #タイムライン取得
        
        pp = pprint.PrettyPrinter(indent=4)
        max_id = last_max_id
        # Tweetを表示
        for result in results:
            # max_id更新
            if max_id < result.id:
                max_id = result.id 
            
           # pp.pprint(result)
            print (result.user.name)
            print (result.text)
            print (result.created_at)
            print (result.id)
            print ("------\r\n")
        
        updateLastMaxId(max_id) # last_max_id更新
        print (last_max_id)
        print (max_id)
        logging.debug("finish collect tweet")
        
    except tweepy.TweepError as e:
        # エラー出力
        
        print(e)
        logging.warning(e)
    
    return 0
    
def updateElasticSearchbyTweets (tweets):
    """
    ElasticsearchへTweetsを更新
    
    Parameters
    -------
    tweets : list
        tweet情報が入ったdictのlist
    
    Returns
    -------
    なし
    
    """
    return 0
    
# 処理開始
main ()

exit
    



