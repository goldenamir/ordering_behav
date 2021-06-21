################################################################
# Importing required libraries

from sqlalchemy import create_engine
import pandas as pd
import numpy as np

################################################################
# ordering function by Pandas 

def orderingFunc(Path):
    Path = 'Path'
    data=pd.read_csv(Path, sep='\t', header = None, usecols=[0,1])
    data.info(verbose=False, memory_usage='deep')

    data_modified=data.sort_values([0,1])
    data_modified = data_modified.drop(0, axis = 0)
    data_modified.columns = ['id', 'Date']
    data_modified.info(verbose=False, memory_usage='deep')

    data_modified.to_csv('data_modified.csv')
    data_modified.info(verbose=False, memory_usage='deep')

################################################################
#  Lookup function 

def lookupFunc():


    ################################################################
    # Creating a database for query on it

    engine = create_engine('sqlite:///ordering.sqlite') 
    print('[INFO]... engine for a database has been generated!')
    print('################################################################')

    ################################################################
    #  Ordering function

    chunk_size_sql=50000 # number of rows in each chunk


    i = 0 # conting the number of bunches

    for chunk in pd.read_csv('largefile.txt', sep = '\t', header = None, names = ['id','date','data'], chunksize=chunk_size_sql,iterator=True):
        chunk.to_sql('chunk_sql'+str(i), engine, if_exists='append')
        i += 1
        
    size_of_bunches = i

    print('[INFO]... number of table which are added to the database is: '+str(size_of_bunches))
    print('################################################################')

    ################################################################
    # Connection to the engine of the created datebase

    con = engine.connect() # connection to the engine of the created datebase
    print('[INFO]... connection to the database has been initiated!')
    print('################################################################')

    chunk_size = 100  # Number of rows which is read in the data_modified

    Zeros = np.zeros((chunk_size,3)) # Making zero series for making temp dataframe with chunk_size

    data_temp = pd.DataFrame(Zeros) # Temp dataframe for saving results from lookup in the databae tables



    data_frame_no = 0 # counter on data_frame_no for chaning every chunk_size

    no_chunk_sql = size_of_bunches  # Number of chunks in the database which lookup will carry out


    num_iter_data_modified = 0
 
    m = 0  # for counting every chunk_size

    for i in range(data_modified.shape[0]):    
        
        
        j = data_modified.iloc[i, 0:2]  # values in data_modified in first and second columns

        
        ID = str(j[0]) 

        
        DATE = j[1]

        
        
        for k in range(no_chunk_sql):
            chunk_sql_count = 'chunk_sql'+str(k) # chunk of sql and its related number
            
            a = con.execute('SELECT * from '+(chunk_sql_count)+' where id = '+'\''+str(ID)+'\''+ 'and date = '+str(DATE))
            b = a.fetchall()
            
            if len(b)!=0:
                

                data_temp.iloc[m,2] = b[0][3]
                data_temp.iloc[m,0] = ID
                data_temp.iloc[m,1] = int(DATE)

                m += 1
                num_iter_data_modified += 1
                if num_iter_data_modified%chunk_size==0:
                    data_temp.to_csv('data_sorted_slice_'+str(data_frame_no)+'.txt',index = False, sep = '\t') 
                    data_temp.to_csv('output.txt',mode='a',index = False, header = False, sep = '\t')
                    data_frame_no += 1
                    data_temp = pd.DataFrame(Zeros)
                    m -= chunk_size
                    num_iter_data_modified = 0
                    print('[INFO]... '+str(data_frame_no) +' has been shaped!')
                break
