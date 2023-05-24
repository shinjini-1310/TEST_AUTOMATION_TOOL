try :

    # ## Importing Libraries
    # <a id='id1'></a>
    # [Index](#idi)

    # In[1]:

    import pandas as pd
    import numpy as np
    from itertools import combinations
    import datetime
    import sys
    from sys import getsizeof
    import pytz

    #importing modules and configuring logging feature

    #importing module
    import logging
      
    #Create and configure logger
    logging.basicConfig(filename="logfile.log",format='%(asctime)s %(message)s',datefmt='%d-%b-%y %H:%M:%S',filemode='a')
      
    #Creating an object
    logger=logging.getLogger()
      
    #Setting the threshold of logger to INFO
    logger.setLevel(logging.INFO)


    # <a id='id2'></a>
    # ## Creating Spark session and making suitable configurations
    # [Index](#idi)

    # In[2]:

    import findspark
    findspark.init()
    import pyspark

    from pyspark.sql.session import SparkSession
    from pyspark.sql import SQLContext
    from pyspark.sql.functions import *
    from pyspark.sql.functions import col, array, when
    import pyspark.sql.functions as f
    from pyspark.sql.types import StringType

    logging.info('{0}'.format('INTEGRATED TEST-CASE EXECUTION FRAMEWORK TRIGGERED'))
    print('{0}'.format('INTEGRATED TEST-CASE EXECUTION FRAMEWORK TRIGGERED'))

    #spark_sess_conf = input("Enter your test case document_name:\n")
    #spark_sess_conf = spark_sess_conf.strip() + '.xlsx'

    spark_sess_conf = sys.argv[1]

    try :
        # reading spark session config file
        test = pd.read_excel(spark_sess_conf)
        test = pd.DataFrame(test)
        logging.info('Spark session setup file {0} found'.format(spark_sess_conf))
        print('Spark session setup file {0} found'.format(spark_sess_conf))
        test.head()
    except:
        logging.info('Spark session setup file {0} does not exist'.format(spark_sess_conf))
        print('Spark session setup file {0} does not exist'.format(spark_sess_conf))

    #assigning value of each config parameter to a variable
        
    try:  

        for index,t in test.iterrows():
        
            app_name = str(t['application name']).strip()
            driver_memory = str(t['driver memory']).strip()
            executor_instances = str(t['executor instances']).strip()
            executor_memory = str(t['executor memory']).strip()
            executor_cores = str(t['executor cores']).strip()
            crossJoin_enabled = str(t['crossJoin_enabled (true/false) ?']).strip()
            
            spark = SparkSession \
                .builder\
                .config('spark.app.name', app_name)\
                .config('spark.driver.memory', driver_memory)\
                .config('spark.executor.instances', executor_instances)\
                .config('spark.executor.memory', executor_memory)\
                .config('spark.executor.cores', executor_cores)\
                .config('spark.sql.crossJoin.enabled', crossJoin_enabled)\
                .config('spark.worker.timeout', '1000000')\
                .getOrCreate()
                
            print("Spark session setup successful")
            logging.info('Spark session setup successful') 
            sqlContext = SQLContext(spark)    
            logging.info('SPARK VERSION : {0}'.format(spark.version))
            print('SPARK VERSION : {0}'.format(spark.version))
        
    except Exception as e :
        print("Spark session setup failed, check your SPARK SESSION SETUP FILE for issues with configuration")
        logging.info("Error : {0}".format(str(e)))
        sys.exit("Exiting CDL Framework")
        logging.info("Exiting CDL Framework")

    # <a id='id3'></a>
    # ## Reading test case document
    # [Index](#idi)

    # In[3]:

    #enter the name of test case document

    #file_name = input("Enter your test case document_name:\n")
    #file_name = file_name.strip() + '.xlsx'

    file_name = sys.argv[2]

    try :
        # reading test-case documnet
        test = pd.read_excel(file_name)
        test = pd.DataFrame(test)
        logging.info('Test case document {0} found'.format(file_name))
        print('Test case document {0} found'.format(file_name))
        test.head()
    except:
        logging.info('File {0} does not exist'.format(file_name))
        print('File {0} does not exist'.format(file_name))


    # In[4]:


    #creating format for evidence file

    evidence_file_name = file_name[:-5] + '_evidence.xlsx'
    writer = pd.ExcelWriter(evidence_file_name,engine='xlsxwriter')


    # <a id='id4'></a>
    # ## Null check for tables
    # [Index](#idi)

    # In[5]:


    def null_check(query,str1):
        
        logging.info("--Starting null check--")
        print("--Starting null check--")
        nval=""
        res=""
        col_list = []
        if str1.upper() == 'ALL':
            
            df = spark.sql('{0} limit 1 '.format(query))  
            col_list = list(df.columns)
            logging.info("--Checking null in all columns--")
            print("--Checking null in all columns--")
            
        else:
            col_list = str1.split(',')
            logging.info('col_list for null_check is : {0}'.format(col_list))
            print('col_list for null_check is : {0}'.format(col_list))
            logging.info("--Checking null in specific columns--")
            print("--Checking null in specific columns--")
            
        for i in col_list:
        
            null_count=spark.sql('Select * from ({0})a_temp where {1} is null'.format(query,i))
            cnt=null_count.count()
            
            if cnt>0:
                
                nval+=str(i)+"["+str(cnt)+"]"+" "
                
        if len(nval)==0:
            nval="No Null values detected."
            res="Pass"
        else:
            nval+=" columns have null values."
            res="Fail"
            
        return nval,res


    # In[6]:

    # <a id='id5'></a>
    # ## Duplicate check for tables
    # [Index](#idi)

    # In[7]:


    def dup_check(tc,query,col_names):    
        
        logging.info("--Starting duplicate check--")
        print("--Starting duplicate check--")
        
        if col_names.upper() == 'ALL':
            df = spark.sql('{0} limit 1'.format(query))
            count=0
            res=""
            col_names = ','.join(df.columns)
        
        
        df1 = spark.sql('Select {1},count(*) from ({0})a group by {1} having count(*)>1'.format(query,col_names))
        
        dup_count=df1.count()
        
        if df1.count()!=0:
            logging.info("--Duplicate found--")
            print("--Duplicate found--")
            df1 = spark.sql('Select {1} from ({0})a group by {1} having count(*)>1  limit 10'.format(query,col_names))
            pdf1 = spark.sql('Select {1},count(*) as cnt from ({0})a group by {1} having count(*)>1  limit 10'.format(query,col_names)).toPandas()
            sheet_name1 = str(tc) + '_dupcheck'
            pdf1.to_excel(writer,sheet_name=sheet_name1 ,index=False)
            count=dup_count
            res="Fail"
            
        else:
            logging.info("--Duplicate not found--")
            print("--Duplicate not found--")
            count=df1.count()
            res="Pass"
            
        return count,res
            
    # <a id='id6'></a>
    # ## Count check for tables
    # [Index](#idi)

    # In[8]:


    def count_check(src_query,tgt_query):
        
        
        logging.info("--Starting count check--")
        print("--Starting count check--")
        df1 = spark.sql("{0}".format(src_query)).toPandas()
        df2 = spark.sql("{0}".format(tgt_query)).toPandas()
        
        
        result = ''
        src_count = df1.iloc[0,0]
        tgt_count = df2.iloc[0,0]
        
        print("source count : {0}".format(src_count))
        print("target count : {0}".format(tgt_count))
        logging.info("source count : {0}".format(src_count))
        logging.info("target count : {0}".format(tgt_count))
        
        if df1.iloc[0,0]==df2.iloc[0,0]:
            logging.info("--Count matches--")
            print("--Count matches--")
            result='Pass'
        else:
            logging.info("--Count does not match--")
            print("--Count does not match--")
            result='Fail'
        
        return src_count,tgt_count,result
        

    # <a id='id13'></a>
    # ## Referential Integrity Check
    # [Index](#idi)

    # In[9]:


    def ref_int(test_case_id,src_query,tgt_query,col):
        
        logging.info("--Starting referential Integrity Constraint--")
        print("--Starting referential Integrity Constraint--")
        
        df1 = spark.sql('{0} where {1} not in ({2}) and {1} is not null limit 10'.format(src_query,col,tgt_query))
        
        rs = ''
        
        if df1.count()==0:
            rs = 'Pass'
            logging.info("--The column follow foreign key properties--")
            print("--The column follow foreign key properties--")
        else:
            rs = 'Fail'
            
            pdf1 = df1.toPandas()
            sheet_name1 = str(test_case_id) + '_ref_int'
            pdf1.to_excel(writer,sheet_name=sheet_name1 ,index=False)
            logging.info("--The column does not follow foreign key properties--")
            print("--The column does not follow foreign key properties--")
          

        return rs


    # <a id='id14'></a>
    # ## Numeric Check
    # [Index](#idi)

    # In[10]:


    def isNumeric(test_case_id,col_name,tgt_query):
        
        logging.info("--Starting Numeric Check--")
        print("--Starting Numeric Check--")
        
        df1 = spark.sql('Select * from ({0})a where cast(cast({1} as bigint) as int) is null limit 10'.format(tgt_query,col_name))
        rs = ''
        if df1.count()==0:
            rs = 'Pass'
            logging.info("--The column has passed numeric check--")
            print("--The column has passed numeric check--")
        else:
            rs = 'Fail'
            
            pdf1 = df1.toPandas()
            sheet_name1 = str(test_case_id) + '_numeric_check'
            pdf1.to_excel(writer,sheet_name=sheet_name1 ,index=False)
            logging.info("--The column fails numeric check--")
            print("--The column fails numeric check--")
          

        return rs
        


    # <a id='id15'></a>
    # ## Non Numeric Check
    # [Index](#idi)

    # In[11]:


    def isNonNumeric(test_case_id,col_name,tgt_query):
        
        logging.info("--Starting Non Numeric Check--")
        print("--Starting Non Numeric Check--")
        
        df1 = spark.sql('Select * from ({0})a where {1} rlike "[0-9]" limit 10'.format(tgt_query,col_name))
        rs = ''
        if df1.count()==0:
            rs = 'Pass'
            logging.info("--The column has passed non numeric check--")
            print("--The column has passed non numeric check--")
        else:
            rs = 'Fail'
            
            pdf1 = df1.toPandas()
            sheet_name1 = str(test_case_id) + '_non_numeric_check'
            pdf1.to_excel(writer,sheet_name=sheet_name1 ,index=False)
            logging.info("--The column fails non numeric check--")
            print("--The column fails non numeric check--")

        return rs


    # <a id='id16'></a>
    # ## Valid value Check
    # [Index](#idi)

    # In[12]:


    def validValueCheck(test_case_id,col_name,valid_values,tgt_query):
        
        logging.info("--Starting Valid Values Check--")
        print("--Starting Valid Values Check--")
        
        df1 = spark.sql('select * from ({0})a where {1} not in ({2}) limit 10'.format(tgt_query,col_name,valid_values))
        rs = ''
        if df1.count()==0:
            rs = 'Pass'
            logging.info("--The column has all valid values--")
            print("--The column has all valid values--")
        else:
            rs = 'Fail'
            
            pdf1 = df1.toPandas()
            sheet_name1 = str(test_case_id) + '_valid_val_check'
            pdf1.to_excel(writer,sheet_name=sheet_name1 ,index=False)
            logging.info("--The column has some invalid values--")
            print("--The column has some invalid values--")
          

        return rs


    # <a id='id17'></a>
    # ## Data Check1
    # [Index](#idi)

    # In[13]:


    def dataCheck1(test_case_id,tgt_query):
        
        logging.info("--Starting Data Check1--")
        print("--Starting Data Check1--")
        
        df1 = spark.sql("{0}".format(tgt_query))
        rs = ''
        if df1.count()==0:
            rs = 'Pass'
            logging.info("--Complete data matches--")
            print("--Complete data matches--")
        else:
            rs = 'Fail'
            #df1.show()
            pdf1 = df1.toPandas()
            sheet_name1 = str(test_case_id) + '_data_check'
            pdf1.to_excel(writer,sheet_name=sheet_name1 ,index=False)
            logging.info("--The column has some invalid values--")
            print("--The column has some invalid values--")
          

        return rs


    # <a id='id8'></a>
    # ## Data check
    # [Index](#idi)

    # In[14]:


    def data_analysis(test_case_id,query1,query2,grain):
        
        logging.info("--Starting data check--")
        print("--Starting data check--")
        from pyspark.sql.functions import col
        df1 = spark.sql('{0}'.format(query1))
        logging.info("--SRC Query Executed--")
        print("--SRC Query Executed--")
        col_name=df1.columns
        df2 = spark.sql('Select * from ({0})b_temp'.format(query2))
        logging.info("--TGT Query Executed--")
        print("--TGT Query Executed--")
     
        # converting grain columns to list
        grain = grain.split(',')
        logging.info('---Done till 1---')
        print('---Done till 1---')
        
        # renaming columns
        df2 = df2.toDF(*col_name)
        rs = ''
        mismatches = ''
        logging.info('---Done till 2---')
        print('---Done till 2---')

        # handling null values
        logging.info('---Done till 3---')
        print('---Done till 3---')
        
        # adding source n target columns
        df1 = df1.withColumn("origin",lit('s'))
        df2 = df2.withColumn("origin",lit('t'))
        logging.info('---Done till 4---')
        print('---Done till 4---')
        
        #aliasing
        t1 = df1.alias('t1')
        t2 = df2.alias('t2')
        logging.info('---Done till 5---')
        print('---Done till 5---')
        

        #checking for common records and mismatches
        having_mismatches = t1.join(t2,grain,how='full')
        logging.info('---Done till 6a---')
        print('---Done till 6a---')

        src_only_df = having_mismatches.where(t2['origin'].isNull())
        tgt_only_df = having_mismatches.where(t1['origin'].isNull())
        
        logging.info("---Done till 6b---")
        print("---Done till 6b---")
        
        logging.info("--Count of records in only source-- {0}".format(src_only_df.count()))
        print("--Count of records in only source-- {0}".format(src_only_df.count()))
        
        logging.info("---Done till 6c---")
        print("---Done till 6c---")
        
        logging.info("--Count of records in only target-- {0}".format(tgt_only_df.count()))
        print("--Count of records in only target-- {0}".format(tgt_only_df.count()))
        
        logging.info('---Done till 7---')
        print('---Done till 7---')    
        
        match = t1.join(t2,col_name,how='inner')
        
        src_only = src_only_df.count()
        tgt_only = tgt_only_df.count()
        logging.info('---Done till 8---') 
        print('---Done till 8---')
        
        if src_only != 0:
            pdf1 = pd.DataFrame(src_only_df.take(10),columns=src_only_df.columns)
            sheet_name1 = str(test_case_id) + '_src_only'
            pdf1.to_excel(writer,sheet_name=sheet_name1 ,index=False)
            
        if tgt_only != 0:
            pdf1 = pd.DataFrame(tgt_only_df.take(10),columns=tgt_only_df.columns)
            sheet_name1 = str(test_case_id) + '_tgt_only'
            pdf1.to_excel(writer,sheet_name=sheet_name1 ,index=False)
       

        #calculating % match
        src = df1.count()
        tgt = df2.count()
        common = match.count()
        logging.info('---Done till 9---')
        print('---Done till 9---')
        logging.info('src count : {0}'.format(src))
        print('src count : {0}'.format(src))
        logging.info('tgt count : {0}'.format(tgt))
        print('tgt count : {0}'.format(tgt))
        logging.info('common : {0}'.format(common))
        print('common : {0}'.format(common))
        
        if src != 0:
            src_match_percent = (common/float(src))*100
        else:
            src_match_percent = 'Source has 0 records'
        
        if tgt != 0:
            tgt_match_percent = (common/float(tgt))*100
        else:
            tgt_match_percent = 'Target has 0 records'
        
        logging.info("--Src Match % --  {0}".format(src_match_percent))
        print("--Src Match % --  {0}".format(src_match_percent))
        logging.info("--Tgt Match % --  {0}".format(tgt_match_percent))
        print("--Tgt Match % --  {0}".format(tgt_match_percent))
        logging.info('---Done till 10---')
        print('---Done till 10---')
        
        if int(tgt_match_percent) == 100 and int(src_match_percent) == 100:
            rs = 'Pass'
            mismatches += 'Complete data match'
            logging.info('{0}'.format(mismatches))
            print('{0}'.format(mismatches))
            mismatches = finding_mismatch_columns(test_case_id,having_mismatches,grain,col_name,t1,t2)
            mismatches = ','.join(mismatches)
        elif src_only == 0 and tgt_only !=0:
            rs = 'Fail'
            mismatches = ' Target has extra records'
            logging.info('{0}'.format(mismatches))
            print('{0}'.format(mismatches))
            mismatches = finding_mismatch_columns(test_case_id,having_mismatches,grain,col_name,t1,t2)
            mismatches = ','.join(mismatches)
            logging.info("--Mismatches-- {0}".format(mismatches))
            print("--Mismatches-- {0}".format(mismatches))
        elif src_only != 0 and tgt_only ==0:
            rs = 'Fail'
            mismatches = ' Source has extra records'
            logging.info('{0}'.format(mismatches))
            print('{0}'.format(mismatches))
            mismatches = finding_mismatch_columns(test_case_id,having_mismatches,grain,col_name,t1,t2)
            mismatches = ','.join(mismatches)
            logging.info("--Mismatches-- {0}".format(mismatches))
            print("--Mismatches-- {0}".format(mismatches))
        elif src_only != 0 and tgt_only!=0:
            rs = 'Fail'
            mismatches += ' Source and Target both has extra records'
            logging.info('{0}'.format(mismatches))
            print('{0}'.format(mismatches))
            mismatches = finding_mismatch_columns(test_case_id,having_mismatches,grain,col_name,t1,t2)
            mismatches = ','.join(mismatches)
            logging.info("--Mismatches-- {0}".format(mismatches))
            print("--Mismatches-- {0}".format(mismatches))
        elif src_only == 0 and tgt_only ==0:
            rs = 'Fail'
            mismatches += ' Source and Target does not have extra records'
            logging.info('{0}'.format(mismatches))
            print('{0}'.format(mismatches))
            mismatches = finding_mismatch_columns(test_case_id,having_mismatches,grain,col_name,t1,t2)
            mismatches = ','.join(mismatches)
            logging.info("--Mismatches-- {0}".format(mismatches))
            print("--Mismatches-- {0}".format(mismatches))
            if mismatches=='': rs = 'Pass'
                
        else:
            rs = ''
            mismatches += ''
        
        return rs,mismatches,src_match_percent,tgt_match_percent
        

    def finding_mismatch_columns(Test_case_id,having_mismatches,grain,col_list,t1,t2):
        
        import pyspark.sql.functions as f
        from pyspark.sql.functions import col, array, when
        logging.info("--Starting mismatch finding--")
        print("--Starting mismatch finding--")
        to_compare=[]
        for c in list(having_mismatches.columns):
            if c not in grain:
                if c != 'origin':
                    to_compare.append(c)
        
        to_compare = set(to_compare)
        to_compare = list(to_compare)
        logging.info("--Starting column level mismatch finding--")
        print("--Starting column level mismatch finding--")
        for name in to_compare:
            having_mismatches = having_mismatches.withColumn(name + "_temp", f.when(f.col("t1." + name) != f.col("t2." + name), f.lit(name)))
        
        logging.info("--Final Format--")
        print("--Final Format--")
        having_mismatches = having_mismatches.withColumn("Mismatches", f.concat_ws(",", *map(lambda name: f.col(name + "_temp"),to_compare))).select("*")
        
        mismatches_df = having_mismatches.select('Mismatches').distinct()
        mismatches_df=mismatches_df.toPandas()
        mismatches = list(mismatches_df['Mismatches'])
        
        if len(list(set(mismatches)))==1 and mismatches==[''] : 
            logging.info("Mismatches list length is : {0} and mismatches = {1}".format(len(mismatches),mismatches))
            print("Mismatches list length is : {0} and mismatches = {1}".format(len(mismatches),mismatches))
        
        else:
            mismatches=','.join(mismatches).split(',')
            mismatches=list(set([i for i in mismatches if i!='']))
            mismatches1 = ','.join(mismatches)
            mismatches2 = list(set(mismatches1.split(',')))
            pdf1 = pd.DataFrame(columns=having_mismatches.columns)
            logging.info("--Mismatches columns-- {0}".format(mismatches2))
            print("--Mismatches columns-- {0}".format(mismatches2))
            having_mismatches.registerTempTable('my_table')
            
            for col in mismatches2:
                if col!='':
                    tpdf1 = spark.sql('Select * from my_table where Mismatches like "%{0}%"'.format(col))
                    tpdf1 = pd.DataFrame(tpdf1.take(2),columns=having_mismatches.columns)
                    pdf1 = pd.concat([tpdf1,pdf1],axis=0)
            
            pdf1.drop_duplicates(inplace=True)
            pdf1=pdf1.loc[:,~pdf1.columns.str.contains('_temp$')]
            
            sheet_name1 = str(test_case_id) + '_mismatch'
            pdf1.to_excel(writer,sheet_name=sheet_name1 ,index=False)
            
        return mismatches
            
     
    # <a id='id9'></a>
    # ## Validating test case document format
    # [Index](#idi)


    # UDF code
    def label_byte(pep,hap):

        if pep is None or hap is None : chc_flag = 'NO'

        else:
        
            val=''
            pep_list=[i.strip() for i in pep.split(';')]
            hap_list = [i.strip() for i in hap.split(';')]
            val=[i for i in pep_list if i in hap_list and i.strip() != '']
            chc_flag = ''
            
            if len(val)>=1 : chc_flag = 'YES'
            else: chc_flag = 'NO'
                
            
        return chc_flag


          
    convertUDF = udf(lambda e:label_byte(pep,hap),StringType()) 
    spark.udf.register("convertUDF",label_byte,StringType())

    # In[48]:


    def form_val(l):
        predef_col=["Execute Flag","Execution Type","Test_case ID","Test case Name","Test case description","Check Type","Step Name","Grain","Column Names","Valid Values","Step Description","Source query","Target query","Source Count","Target Count","Expected Result","Actual Result","Status","% Src Data match","% Tgt Data match","Mismatches","Execution Status","Execution Timestamp"]
        if l==predef_col: return 'Test case document format correct'
        else: return 'Test case document format not correct'
        
    form_val(list(test.columns))


    # <a id='id10'></a>
    # ## Test case execution
    # [Index](#idi)

    # In[ ]:


    p = 0
    f = 0
    error = 0
    not_executed=0
    ist = pytz.timezone('Asia/Kolkata') 

    for index,t in test.iterrows():
        
        type_check=t['Step Description']
        src_query  = t['Source query']
        tgt_query  = t['Target query']
        grain = t['Grain']
        column_names = t['Column Names']
        test_case_id = t['Test_case ID']
        execute_flag = t['Execute Flag']
        execution_type = t['Execution Type']
        valid_values = t['Valid Values']
        
        var1 = datetime.datetime.now()
        
        exec_start=datetime.datetime.now()
        exec_start=exec_start.strftime("%Y-%m-%d %H:%M:%S.%f")
        test.loc[index,'Execution Timestamp']=exec_start
        
        if execute_flag.upper() == 'Y' and execution_type.upper() == 'AUTOMATED':
            logging.info('#############')
            print('#############')
            logging.info('Starting execution for test case no----{0}'.format(t['Test_case ID']))
            print('Starting execution for test case no----{0}'.format(t['Test_case ID']))
            if type_check.lower()=='null_check':

                try:
                    cols,rs= null_check(tgt_query,column_names)
                    test.loc[index,'Status'] = rs
                    test.loc[index,'Execution Status']='Executed'

                    if rs=='Pass':
                        p += 1
                        test.loc[index,'Actual Result']="The columns don't have null values"

                    else:
                        f += 1
                        test.loc[index,'Actual Result']=cols

                    logging.info("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                    print("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                except Exception as e :            
                    l=str(e).split(';')
                    test.loc[index,'Execution Status']='Please check your null query'
                    logging.info("Error : {0}".format(l[0]))
                    print("Error : {0}".format(test.loc[index,'Execution Status']))
                    error += 1

            elif type_check.lower()=='dup_check':

                try:
                    dcount,rs= dup_check(test_case_id,tgt_query,grain)

                    test.loc[index,'Status']=rs

                    test.loc[index,'Execution Status']='Executed'


                    if test.loc[index,'Status']=='Pass':
                        p += 1
                        test.loc[index,'Target Count']=dcount
                        test.loc[index,'Actual Result']= 'The given query did not return any duplicates'
                    else:
                        f += 1
                        test.loc[index,'Target Count']=dcount


                        test.loc[index,'Actual Result']= 'The given query returned duplicates'

                    logging.info("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                    print("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                except Exception as e : 
                    l=str(e).split(';')
                    test.loc[index,'Execution Status']='Please check your duplicate query'
                    logging.info("Error : {0}".format(l[0]))
                    print("Error : {0}".format(test.loc[index,'Execution Status']))
                    error += 1

            elif type_check.lower()=='count_check':

                try:
                    src_count,tgt_count,rs = count_check(src_query,tgt_query)
                    test.loc[index,'Source Count']=src_count
                    test.loc[index,'Target Count']=tgt_count
                    test.loc[index,'Status'] = rs
                    test.loc[index,'Execution Status']='Executed'

                    if rs == 'Pass':
                        p += 1
                        test.loc[index,'Actual Result']= 'The count matches for both tables.'
                    else:
                        f += 1
                        test.loc[index,'Actual Result']= 'The count does not match for both tables.'
                    logging.info("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                    print("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                except Exception as e : 
                    l=str(e).split(';')
                    test.loc[index,'Execution Status']='Please check your count query'
                    logging.info("Error : {0}".format(l[0]))
                    print("Error : {0}".format(test.loc[index,'Execution Status']))
                    error += 1

            elif type_check.lower()=='data_check':

                try:
                    rs,mis,smp,tmp = data_analysis(test_case_id,src_query,tgt_query,grain)

                    test.loc[index,'Status'] = rs
                    test.loc[index,'% Src Data match'] = smp
                    test.loc[index,'% Tgt Data match'] = tmp
                    test.loc[index,'Mismatches'] = mis

                    if rs =='Pass':
                        p += 1
                        test.loc[index,'Actual Result']= 'The data matches between source and target.'
                    else:
                        f += 1
                        test.loc[index,'Actual Result']= 'The data does not match between source and target.'

                    test.loc[index,'Execution Status']='Executed'
                    logging.info("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                    print("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                except Exception as e : 
                    l=str(e).split(';')
                    test.loc[index,'Execution Status']='Please check your data query'
                    logging.info("Error : {0}".format(l[0]))
                    print("Error : {0}".format(test.loc[index,'Execution Status']))
                    error += 1

            elif type_check.lower()=='ref_check':

                try: 
                    rs=ref_int(test_case_id,src_query,tgt_query,column_names)
                    test.loc[index,'Status'] = rs            
                    if rs =='Pass':
                        p += 1
                        test.loc[index,'Actual Result']='The source table passed the refrential integrity check'
                    else:
                        f += 1
                        test.loc[index,'Actual Result']='The source table failed the refrential integrity check'

                    test.loc[index,'Execution Status']='Executed'
                    logging.info("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                    print("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                except Exception as e : 
                    l=str(e).split(';')
                    test.loc[index,'Execution Status']='Please check your referential integrity check query'
                    logging.info("Error : {0}".format(l[0]))
                    print("Error : {0}".format(test.loc[index,'Execution Status']))
                    error += 1
            
            elif type_check.lower()=='num_check':

                try:
                    rs= isNumeric(test_case_id,column_names,tgt_query)
                    test.loc[index,'Status']=rs
                    test.loc[index,'Execution Status']='Executed'

                    if test.loc[index,'Status']=='Pass':
                        p += 1
                        test.loc[index,'Actual Result']= 'The given query did not return non-numeric values'
                    else:
                        f += 1
                        test.loc[index,'Actual Result']= 'The given query returned non-numeric values'

                    logging.info("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                    print("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                except Exception as e : 
                    l=str(e).split(';')
                    test.loc[index,'Execution Status']='Please check your numeric query'
                    logging.info("Error : {0}".format(l[0]))
                    print("Error : {0}".format(test.loc[index,'Execution Status']))
                    error += 1
                    
            elif type_check.lower()=='non_num_check':

                try:
                    rs= isNonNumeric(test_case_id,column_names,tgt_query)
                    test.loc[index,'Status']=rs
                    test.loc[index,'Execution Status']='Executed'

                    if test.loc[index,'Status']=='Pass':
                        p += 1
                        test.loc[index,'Actual Result']= 'The given query did not return numeric values'
                    else:
                        f += 1
                        test.loc[index,'Actual Result']= 'The given query returned numeric values'

                    logging.info("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                    print("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                except Exception as e : 
                    l=str(e).split(';')
                    test.loc[index,'Execution Status']='Please check your non-numeric query'
                    logging.info("Error : {0}".format(l[0]))
                    print("Error : {0}".format(test.loc[index,'Execution Status']))
                    error += 1
                    
            elif type_check.lower()=='valid_value_check':

                try:
                    rs= validValueCheck(test_case_id,column_names,valid_values,tgt_query)
                    test.loc[index,'Status']=rs
                    test.loc[index,'Execution Status']='Executed'

                    if test.loc[index,'Status']=='Pass':
                        p += 1
                        test.loc[index,'Actual Result']= 'The specified column(s) have valid values'
                    else:
                        f += 1
                        test.loc[index,'Actual Result']= 'The specified column(s) do not have valid values'

                    logging.info("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                    print("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                except Exception as e : 
                    l=str(e).split(';')
                    test.loc[index,'Execution Status']='Please check your valid value query'
                    logging.info("Error : {0}".format(l[0]))
                    print("Error : {0}".format(test.loc[index,'Execution Status']))
                    error += 1
                    
            elif type_check.lower()=='data_check1':

                try:
                    rs= dataCheck1(test_case_id,tgt_query)
                    test.loc[index,'Status']=rs
                    test.loc[index,'Execution Status']='Executed'

                    if test.loc[index,'Status']=='Pass':
                        p += 1
                        test.loc[index,'Actual Result']= 'The given query did not return invalid records'
                    else:
                        f += 1
                        test.loc[index,'Actual Result']= 'The given query returned invalid records'

                    logging.info("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                    print("Status: {0} Result: {1}".format(rs,test.loc[index,'Actual Result']))
                except Exception as e : 
                    l=str(e).split(';')
                    test.loc[index,'Execution Status']='Please check your data check1 query'
                    logging.info("Error : {0}".format(l[0]))
                    print("Error : {0}".format(test.loc[index,'Execution Status']))
                    error += 1
            else:

                error += 1
                
                test.loc[index,'Execution Status']='Invalid Step Description'


            logging.info('Test case id ' + str(t['Test_case ID']) + ' has been executed')
            print('Test case id ' + str(t['Test_case ID']) + ' has been executed')
            var2 = datetime.datetime.now()
            time_delta=var2-var1
            logging.info("--Time taken-- {0}".format(time_delta))
            print("--Time taken-- {0}".format(time_delta))
            logging.info('#############')
            print('#############')
            print()
                
        else:
            not_executed+=1
            test.loc[index,'Execution Status']='Invalid Execute Flag/Invalid Execution Type'


    # <a id='id11'></a>
    # ## Summary generation
    # [Index](#idi)

    # In[70]:


    logging.info('Test cases passed {0}'.format(p))
    print('Test cases passed {0}'.format(p))
    logging.info('Test cases failed {0}'.format(f))
    print('Test cases failed {0}'.format(f))
    logging.info('Test cases with error {0}'.format(error))
    print('Test cases with error {0}'.format(error))
    logging.info('Test cases not executed {0}'.format(not_executed))
    print('Test cases not executed {0}'.format(not_executed))

    logging.info('{0}'.format('TEST-CASES EXECUTION COMPLETE'))
    print('{0}'.format('TEST-CASES EXECUTION COMPLETE'))

    # <a id='id12'></a>
    # ## Saving the test results
    # [Index](#idi)

    # In[71]:


    test.fillna('NA', inplace = True)
    test.to_excel(writer,sheet_name='Results',index=False)
    writer.save()
    writer.close()
    
except KeyboardInterrupt as e :

    l=str(e).split(';')
    logging.info("Error : {0}".format(l[0]))
    print("Error : {0}".format("KeyboardInterrupt Exception Occurred"))
    
    test.fillna('NA', inplace = True)
    test.to_excel(writer,sheet_name='Results',index=False)
    writer.save()
    writer.close()