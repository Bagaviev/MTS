def archivator(user, input_sp, task):
    from os.path import basename
    import os, errno
    import zipfile
    import shutil
    path_csv = '/home/{}/seg{}/csv'.format(user, task)
    path_result = '/home/{}/seg{}'.format(user, task)
    
    try:
        os.makedirs(path_csv)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    print("Directory /home/seg{} was created!".format(task))
    
    input_pd = input_sp.toPandas()
    input_pd.to_csv(path_csv + '/seg{}.csv'.format(task), index = False, header = False)
    
    zf = zipfile.ZipFile(path_result + '/seg{}.zip'.format(task), "w", zipfile.ZIP_DEFLATED)
    for dirname, subdirs, files in os.walk(path_csv):
        for filename in files:
            zf.write(os.path.join(dirname, filename), basename(filename))
    zf.close()

    shutil.rmtree(path_csv)
    print("d0ne")