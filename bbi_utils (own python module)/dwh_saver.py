def dwh_saver(df, task_num, label, persistent, smol, partition):
	assert isinstance(label, str), 'label should be True or False (String)'
	assert isinstance(persistent, str), 'persistent should be True or False (String)'
	assert isinstance(task_num, str), 'task_num should be True or False (String)'
	assert isinstance(partition, str), 'partition should be True or False (String)'
	if persistent=='True':
		if smol=='True':
			df.repartition(10).write.mode('overwrite').format("orc").saveAsTable('advert_sb.bbi_seg{}_{}'.format(task_num, label))
			print('Table saved at advert_sb.bbi_seg{}_{}'.format(task_num, label))
		elif smol=='False':
			df.repartition("{}".format(partition)).write.mode('overwrite').format("orc").partitionBy("{}".format(partition)).saveAsTable('advert_sb.bbi_seg{}_{}'.format(task_num, label))
			print('Table saved at advert_sb.bbi_seg{}_{}'.format(task_num, label))
		else: print ("Smol or not?")
	elif persistent=='False':
		df.createOrReplaceTempView('bbi_seg{}_{}'.format(task_num, label))
		print('Table in-memory at bbi_seg{}_{}'.format(task_num, label))
	else: print ("Persistent or not?")