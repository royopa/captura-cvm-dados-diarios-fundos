#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import os
import pyodbc
from . import sqlserverport
from sqlalchemy import create_engine

from dotenv import load_dotenv
from dotenv import find_dotenv
load_dotenv(find_dotenv())


def get_conn_sirat():
    server = os.environ.get('database_host')
    database = os.environ.get('database_name')
    username = os.environ.get('database_user')
    password = os.environ.get('database_password')
    driver = os.environ.get('database_driver')
    conn = pyodbc.connect('DRIVER={'+driver+'};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    return conn


def get_conn_maps():
    # Some other example server values are
    server = 'SP7877SR020'
    database = 'MAPS'  # alterar conforme a necessidade
    username = 'sirat'
    password = 'G3r@Ts1R@t'
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect('DRIVER={'+driver+'};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    return conn   


def get_conn_postgres():
    engine = create_engine('postgresql+psycopg2://usr_gerat:usr_gerat@10.4.2.209:5432/GERAT')
    return engine    