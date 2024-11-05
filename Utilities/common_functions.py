# Databricks notebook source
def f_get_secret(key):
    try:
        return dbutils.secrets.get(scope="cryptoproject", key=key)
    except Exception as e:
        raise(e)
