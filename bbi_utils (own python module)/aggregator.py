import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType
from spark_py_utils.clickstream import join_to_clickstream
	
def aggregator_v3(spark, task_num, input_excel, label, dt1, dt2, no_agregation):
	from bbi_utils.dwh_saver import dwh_saver
	spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

	df = pd.read_excel(input_excel, dtype = object, header = None)
	df.columns = ['host_name','seg']

	schema = StructType([StructField('host_name', StringType(), True), StructField('seg', StringType(), True)])
	df_f = spark.createDataFrame(df, schema)
	df_f.select("host_name", "seg").distinct().groupby("seg").count().orderBy('seg', ascending=True).show()
	if no_agregation=='True':
		dwh_saver(join_to_clickstream(spark, hosts_df=df_f, start_dt=dt1, end_dt=dt2, no_months=True, no_weeks=True), task_num, label=label, persistent='True', smol='False', partition='agg_date')
	elif no_agregation=='False':
		dwh_saver(join_to_clickstream(spark, hosts_df=df_f, start_dt=dt1, end_dt=dt2, no_months=False, no_weeks=False), task_num, label=label, persistent='True', smol='False', partition='agg_date')
	else: print ("no_agregation?")
	return spark.sql("select seg, count(distinct msisdn) cnt from advert_sb.bbi_seg{}_{} group by seg".format(task_num, label)).show()