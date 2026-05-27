-- Databricks notebook source

select * from ds_catalog_dev.credit_engine.abt_inference_me_br where id_customer = 11175

-- COMMAND ----------

select * from ds_catalog_dev.credit_engine.apply_model_me_br where id_customer = 11175