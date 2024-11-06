# Databricks notebook source
# MAGIC %%sql
# MAGIC OPTIMIZE events ZORDER BY (eventType)
