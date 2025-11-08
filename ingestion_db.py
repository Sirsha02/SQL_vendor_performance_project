#THIS CODE IS OK FOR NORMAL-SIZED FILES, crashes for huge datasets
# import pandas as pd
# import os
# import sqlalchemy as sal
# import logging
# import time

# logging.basicConfig(
#     filename = 'logs/ingestion_db.log',
#     level = logging.DEBUG,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     filemode='a'
# )

# engine = sal.create_engine('sqlite:///vendor_perf_project.db')

# def ingest_db(df, table_name, engine):
#     '''This function will ingest the df into database table'''
#     df.to_sql(table_name, con=engine, if_exists = 'replace', index=False)

# def load_raw_data():
#     '''This function will load the CSVs as dataframe and ingest in db'''
#     start = time.time()
#     for file in os.listdir('data'):
#         if '.csv' in file:
#             df =pd.read_csv('data/'+file)
#             logging.info(f'Ingesting {file} in db')
#             ingest_db(df, file[:-4], engine)
#     end = time.time()
#     total_time = (end - start)/60
#     logging.info('--------------Ingestion Complete--------------')
#     logging.info(f'\nTotal Time Taken : {total_time} minutes')


# if __name__ =='__main__':
#     load_raw_data()


#PROCESSES HUGE DATASETS
import pandas as pd
import os
import sqlalchemy as sal
import logging
import time

# Setup logging
logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode='a'
)

# SQLite engine
engine = sal.create_engine('sqlite:///vendor_perf_project.db')

def ingest_db_in_chunks(file_path, table_name, engine, chunksize=50000):
    """Ingest large CSVs into the database in chunks."""
    logging.info(f"Starting chunked ingestion for {file_path}")
    start = time.time()
    chunk_iter = pd.read_csv(file_path, chunksize=chunksize)
    first_chunk = True
    
    for i, chunk in enumerate(chunk_iter):
        chunk.to_sql(
            table_name,
            con=engine,
            if_exists='replace' if first_chunk else 'append',
            index=False
        )
        logging.info(f"Inserted chunk {i+1} for {table_name}")
        first_chunk = False
    
    end = time.time()
    total_time = (end - start)/60
    logging.info(f"Completed ingestion for {file_path}. Time taken: {total_time:.2f} minutes.")

def load_raw_data():
    """Load the CSVs as dataframe and ingest in db."""
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            try:
                logging.info(f"Ingesting {file} into db")
                # Use chunked ingestion for large files
                if os.path.getsize(file_path) > 200 * 1024 * 1024:  # >200MB
                    ingest_db_in_chunks(file_path, file[:-4], engine)
                else:
                    df = pd.read_csv(file_path)
                    df.to_sql(file[:-4], con=engine, if_exists='replace', index=False)
            except Exception as e:
                logging.error(f"Error processing {file}: {e}", exc_info=True)
    
    end = time.time()
    total_time = (end - start)/60
    logging.info("--------------Ingestion Complete--------------")
    logging.info(f"Total Time Taken : {total_time:.2f} minutes")

if __name__ == '__main__':
    load_raw_data()
