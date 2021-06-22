################################################################
# Importing required libraries

from sqlalchemy import create_engine
import pandas as pd
import numpy as np


################################################################
# ordering function by Pandas

'''

The aim of faunction is making a function which make a Pandas DataFrame
and sort first based on 'id' colmn and second based on 'date' colum which
is 'time stamp' in this project. 

Regards to making a fast sorting just id and date columns have been called
to the dataframe. 

Generated dataframe is called 'data_modified' dataframe and regards to make a scalable 
usage from data_modified, it has been saved as .csv file. 

'''


def orderingFunc(Path):
    data = pd.read_csv(Path, sep='\t', header=None, usecols=[0, 1])
    data.info(verbose=False, memory_usage='deep')

    data_modified = data.sort_values([0, 1])
    data_modified = data_modified.drop(0, axis=0)
    data_modified.columns = ['id', 'Date']

    print('################################################################')
    print('[INFO]... Checking the memory usage for CREATING a dataframe!')
    print('################################################################')
    data_modified.info(verbose=False, memory_usage='deep')

    print('[INFO]... Checking the memory usage for SAVING a dataframe!')
    print('################################################################')
    data_modified.to_csv('data_modified.csv')
    data_modified.info(verbose=False, memory_usage='deep')


################################################################
#  Lookup function

'''

In lookupfunc, the major aim is generating a database based on sqlite since
sqlite database is 50 times faster than normal libraries like Pandas and Dask.
However, Pandas and Dask were tested in this project. I figured it out that 
making a database is faster in contract to oder solutions. 

Steps for developing the function:

1/ an egnine has been created on python which is called 'engine',
2/ an sqlite database with 50,000 rows has been grenerated. There is a 
posbility to change the number of rows or tables sizes with modification
on the value of 'chunk_size_sql'.
3/ connection to the engine has been stablished.
4/ based on the sorted dataframe, query on the tables of the database has
been carried out and result were saved on two csv files.The first csv file 
save every 100 results which it got results and second csv which is called
output.csv saves whole sorted datas. 

Whenever, a quarry got a result, it will be saved and then will be removed 
from the database.

'''


def lookupFunc():
    ################################################################
    # Calling the dataframe
    data_modified = pd.read_csv('data_modified.csv')

    ################################################################
    # Creating a database for query on it

    engine = create_engine('sqlite:///ordering.sqlite')
    print('[INFO]... engine for a database has been generated!')
    print('################################################################')

    ################################################################
    #  Ordering function

    chunk_size_sql = 50000  # number of rows in each chunk

    bunch_sql = 0  # counting the number of bunches

    for chunk in pd.read_csv('largefile.txt', sep='\t', header=None, names=['id', 'date', 'data'],
                             chunksize=chunk_size_sql, iterator=True):
        chunk.to_sql('chunk_sql' + str(bunch_sql), engine, if_exists='append')
        bunch_sql += 1

    size_of_bunches = bunch_sql

    print('[INFO]... number of table which are added to the database is: ' +
          str(size_of_bunches))
    print('################################################################')

    ################################################################
    # Connection to the engine of the created datebase

    con = engine.connect()  # connection to the engine of the created datebase
    print('[INFO]... connection to the database has been initiated!')
    print('################################################################')

    chunk_size = 100  # Number of rows which is read in the data_modified

    # Making zero series for making temp dataframe with chunk_size
    Zeros = np.zeros((chunk_size, 3))

    # Temp dataframe for saving results from lookup in the database tables
    data_temp = pd.DataFrame(Zeros)

    data_frame_no = 0  # counter on data_frame_no for chaning every chunk_size

    # Number of chunks in the database which lookup will carry out
    no_chunk_sql = size_of_bunches

    num_iter_data_modified = 0

    chunk_el = 0  # for counting every chunk_size

    # For loop in the all rows of sorted dataframe

    for sub_data_modified in range(data_modified.shape[0]):

        # values in data_modified in first and second columns
        column_data_modified = data_modified.iloc[sub_data_modified, 0:3]
        ID = str(column_data_modified[1])

        DATE = column_data_modified[2]

        for sub_chunk_sql in range(no_chunk_sql):
            # chunk of sql and its related number
            chunk_sql_count = 'chunk_sql' + str(sub_chunk_sql)

            a = con.execute('SELECT * from '+(chunk_sql_count) +
                            ' where id = '+"'"+str(ID)+"'"+' AND date = '+str(DATE))
            b = a.fetchall()

            if len(b) != 0:
                # print('[INFO]... result has been fund!')
                # print('################################################################')
                data_temp.iloc[chunk_el, 0] = b[0][1]
                data_temp.iloc[chunk_el, 1] = int(b[0][2])
                data_temp.iloc[chunk_el, 2] = b[0][3]

                # Deleting a row from the database
                #con.execute("DELETE FROM %s where id == %s AND date == %s" % (chunk_sql_count, ID, DATE))
                con.execute('DELETE  from '+(chunk_sql_count) +
                            ' where id = '+"'"+str(ID)+"'"+' AND date = '+str(DATE))
                chunk_el += 1
                num_iter_data_modified += 1
                if num_iter_data_modified % chunk_size == 0:
                    # data_temp.to_csv(
                    #     'data_sorted_slice_' + str(data_frame_no) + '.txt', index=False,header = False, sep='\t')
                    data_temp.to_csv('output.txt', mode='a',
                                     index=False, header=False, sep='\t')
                    data_frame_no += 1
                    data_temp = pd.DataFrame(Zeros)
                    chunk_el = 0
                    num_iter_data_modified = 0
                    print('[INFO]... ' + str(data_frame_no) +
                          ' has been shaped!')
                break
