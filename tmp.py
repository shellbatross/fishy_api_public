"""
Working with the data for the api here, this is only really to turn the csvs into a table for each "wave" which is a two month period
****necessary to have different tables for each two months to make the query of a user as accurate as possible depending on the fishing season
These are in a sqlite3 db, don't really use this one for any storing as of now, just to grab information
"""
from sqlalchemy import create_engine
import pymysql
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base

engine = create_engine("")
import sqlite3
import os 
from sqlite3 import Error
import pandas as pd
c = engine.connect()
csvs = []
df_wave1,df_wave2,df_wave3,df_wave4,df_wave5,df_wave6 = pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
directory = os.fsencode("/home/shell/fishy_api/catch_datasets")

for file in os.listdir(directory):
    tmp = os.fsdecode(file)
    file_loc = os.path.join(os.fsdecode(directory),tmp)
    if tmp[10] == "1":
        if df_wave1.empty:
            df_wave1 = pd.read_csv(file_loc, low_memory = False, sep=',', index_col=False, dtype='unicode')
        else:
            try:
                meow= pd.read_csv(file_loc, low_memory=False, sep=',', index_col=False, dtype='unicode')
                df_wave1 = pd.concat([df_wave1,meow])
            except:
                print("I don't wanna error, but go check" + str(file_loc))
                continue
    if tmp[10] == "2":
        if df_wave2.empty:
            df_wave2 = pd.read_csv(file_loc, low_memory = False, sep=',', index_col=False, dtype='unicode')
        else:
            try:
                meow= pd.read_csv(file_loc, low_memory=False, sep=',', index_col=False, dtype='unicode')
                df_wave2 = pd.concat([df_wave2,meow])
            except:
                print("I don't wanna error, but go check" + str(file_loc))
                continue
    if tmp[10] == "3":
        if df_wave3.empty:
            df_wave3 = pd.read_csv(file_loc, low_memory = False, sep=',', index_col=False, dtype='unicode')
        else:
            try:
                meow= pd.read_csv(file_loc, low_memory=False, sep=',', index_col=False, dtype='unicode')
                df_wave3 = pd.concat([df_wave3,meow])
            except:
                print("I don't wanna error, but go check" + str(file_loc))
                continue
    if tmp[10] == "4":
        if df_wave4.empty:
            df_wave4 = pd.read_csv(file_loc, low_memory = False, sep=',', index_col=False, dtype='unicode')
        else:
            try:
                meow= pd.read_csv(file_loc, low_memory=False, sep=',', index_col=False, dtype='unicode')
                df_wave4 = pd.concat([df_wave4,meow])
            except:
                print("I don't wanna error, but go check" + str(file_loc))
                continue
    if tmp[10] == "5":
        if df_wave5.empty:
            df_wave5 = pd.read_csv(file_loc, low_memory = False, sep=',', index_col=False, dtype='unicode')
        else:
            try:
                meow= pd.read_csv(file_loc, low_memory=False, sep=',', index_col=False, dtype='unicode')
                df_wave5= pd.concat([df_wave5,meow])
            except:
                print("I don't wanna error, but go check" + str(file_loc))
                continue
    if tmp[10] == "6":
        if df_wave6.empty:
            df_wave6 = pd.read_csv(file_loc, low_memory = False, sep=',', index_col=False, dtype='unicode')
        else:
            try:
                meow= pd.read_csv(file_loc, low_memory=False, sep=',', index_col=False, dtype='unicode')
                df_wave6= pd.concat([df_wave6,meow])
            except:
                print("I don't wanna error, but go check" + str(file_loc))
                continue



#Get rid of all duplicate columns in every df
df_wave1.columns = map(str.lower, df_wave1.columns)
df_wave2.columns = map(str.lower, df_wave2.columns)
df_wave3.columns = map(str.lower, df_wave3.columns)
df_wave4.columns = map(str.lower, df_wave4.columns)
df_wave5.columns = map(str.lower, df_wave5.columns)
df_wave6.columns = map(str.lower, df_wave6.columns)
df_wave1= df_wave1.loc[:,~df_wave1.columns.duplicated()].copy()
df_wave2= df_wave2.loc[:,~df_wave2.columns.duplicated()].copy()
df_wave3= df_wave3.loc[:,~df_wave3.columns.duplicated()].copy()
df_wave4= df_wave4.loc[:,~df_wave4.columns.duplicated()].copy()
df_wave5= df_wave5.loc[:,~df_wave5.columns.duplicated()].copy()
df_wave6= df_wave6.loc[:,~df_wave6.columns.duplicated()].copy()
df_wave1.drop(["fl_reg","harvest","id_code","psu_id","release","sp_code","strat_id","strat_interval","tot_cat","tot_len","tot_len_a","tot_len_b1",
               "wgt_a","wgt_ab1","wgt_b1","wp_int","kod","imp_rec","wp_catch_precal","wp_int_precal","date_published","alt_flag","arx_method","var_id",
               "wp_catch","release_unadj","harvest_unadj","claim_unadj","claim"
               ], axis =1, inplace = True)
df_wave2.drop(["fl_reg","harvest","id_code","psu_id","release","sp_code","strat_id","strat_interval","tot_cat","tot_len","tot_len_a","tot_len_b1",
               "wgt_a","wgt_ab1","wgt_b1","wp_int","kod","imp_rec","wp_catch_precal","wp_int_precal","date_published","alt_flag","arx_method","var_id",
               "wp_catch","release_unadj","harvest_unadj","claim_unadj","claim"
               ], axis =1, inplace = True)
df_wave3.drop(["fl_reg","harvest","id_code","psu_id","release","sp_code","strat_id","strat_interval","tot_cat","tot_len","tot_len_a","tot_len_b1",
               "wgt_a","wgt_ab1","wgt_b1","wp_int","kod","imp_rec","wp_catch_precal","wp_int_precal","date_published","alt_flag","arx_method","var_id",
               "wp_catch","release_unadj","harvest_unadj","claim_unadj","claim"
               ], axis =1, inplace = True)
df_wave4.drop(["fl_reg","harvest","id_code","psu_id","release","sp_code","strat_id","strat_interval","tot_cat","tot_len","tot_len_a","tot_len_b1",
               "wgt_a","wgt_ab1","wgt_b1","wp_int","kod","imp_rec","wp_catch_precal","wp_int_precal","date_published","alt_flag","arx_method","var_id",
               "wp_catch","release_unadj","harvest_unadj","claim_unadj","claim"
               ], axis =1, inplace = True)
df_wave5.drop(["fl_reg","harvest","id_code","psu_id","release","sp_code","strat_id","strat_interval","tot_cat","tot_len","tot_len_a","tot_len_b1",
               "wgt_a","wgt_ab1","wgt_b1","wp_int","kod","imp_rec","wp_catch_precal","wp_int_precal","date_published","alt_flag","arx_method","var_id",
               "wp_catch","release_unadj","harvest_unadj","claim_unadj","claim"
               ], axis =1, inplace = True)
df_wave6.drop(["fl_reg","harvest","id_code","psu_id","release","sp_code","strat_id","strat_interval","tot_cat","tot_len","tot_len_a","tot_len_b1",
               "wgt_a","wgt_ab1","wgt_b1","wp_int","kod","imp_rec","wp_catch_precal","wp_int_precal","date_published","alt_flag","arx_method","var_id",
               "wp_catch","release_unadj","harvest_unadj","claim_unadj","claim"
               ], axis =1, inplace = True)

               
df_wave1.to_sql(name='wave1', con=engine)
df_wave2.to_sql(name='wave2', con=engine)
df_wave3.to_sql(name='wave3', con=engine)
df_wave4.to_sql(name='wave4', con=engine)
df_wave5.to_sql(name='wave5', con=engine)
df_wave6.to_sql(name='wave6', con=engine)
"""
c.execute(
    '''CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password BLOB NOT NULL
);'''   
)

#lol idkno the longest fish name
c.execute(
    '''CREATE TABLE trips(
   trip_id INTEGER PRIMARY KEY,
   address TEXT NOT NULL,
   Fish TEXT NOT NULL,
   wave TEXT NOT NULL,
   state TEXT NOT NULL,
   date TEXT NOT NULL

);'''
)

c.execute(
    '''CREATE TABLE user_trips(
    user_id INTEGER,
    trip_id INTEGER,
    PRIMARY KEY(user_id,trip_id)
    FOREIGN KEY(user_id)
        REFERENCES users(user_id)   
        ON DELETE CASCADE
        ON UPDATE NO ACTION,
    FOREIGN KEY(trip_id)
        REFERENCES trips(trip_id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION
);'''
)
"""
c.close()