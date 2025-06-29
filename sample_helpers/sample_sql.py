import sqlite3
import pandas as pd
from glob import glob

class sample_tables:
    def __init__(self, csv_path = 'CSV_data/', db_path = 'Database/sample.db'):
        # class storage for paths and sqlite objects
        self.db_path = db_path
        self.csv_path = csv_path
        self.connection = None
        self.cursor = None

        # creating a mapping for which csv belongs to which table
        #   realistically, I'd have written a function that maps csvs to tables based on regex 
        #   or character matching but this seemed quicker for now.
        self.csv_to_table_dict = {'customer_offer_rewards_144392.csv':'customer_offer_rewards',
                             'offer_rewards_168083.csv':'offer_rewards',
                            'customer_offers_296332.csv':'customer_offers',
                            'customer_offer_redemptions_31025.csv':'customer_offer_redemptions'}
    
    def _connect(self):
        '''
        Internal class function to connect to the sqlite database located in db_path
        '''
        self.connection = sqlite3.connect(self.db_path) #connect to DB
        self.cursor =  self.connection.cursor() #create cursor object
    
    def _close_cursor(self):
        '''
        Internal class to close cursor
        '''
        self.cursor.close() #close cursor

    def _truncate_tables(self):
        '''
        Internal class function to delete contents from required tables
        '''
        self._connect() #connect to DB and cursor
        
        tables = [i for i in self.csv_to_table_dict.values()] #iterate over tables in database
        for table in tables:
            #delete given table
            sql_query = f"DELETE FROM {table};"
            self.cursor.execute(sql_query)
        
        self.connection.commit() # commit changes to DB
        self._close_cursor() # close cursor object

    def query(self, query, ret = True):
        '''
        Run a SQL query

        Parameters:
                query (str): a SQL query to run
                ret (bool): True to return the result of query
        '''
        self._connect() #connect to DB and cursor
        self.cursor.execute(query) #execute query
        if ret:
            return self.cursor.fetchall() #if ret is TRUE (default), then return the results

    def build_dbs(self):
        '''
        Build all CSVs located in CSV_path to sqlite databases
        '''
        self._connect()  #connect to DB and cursor

        csvs_to_read = glob(self.csv_path + "/*.csv")  #path of CSVs 

        try:
            for csv_file in csvs_to_read:  #for each CSV 
                csv_file_name = csv_file.split("/")[-1]  #get the name of the CSV file
                table_to_write = self.csv_to_table_dict.get(csv_file_name) #store table name as var table_to_write

                temp_df = pd.read_csv(csv_file) # read in CSV from path

                temp_df.to_sql(table_to_write, self.connection, if_exists='append',  index=False) # pandas builtin for sql dbs

        except sqlite3.IntegrityError:
            # I wasn't sure if someone would try to execute my code, if so, there will be an error because the data already exists.
            # I would have wrapped this in a decorator, but felt that would take more time.
                print("USER INPUT REQUIRED:")
                print(" It looks like the database has already populated!")
                print(" Would you like to truncate the tables and rebuild the Database?")
                print(" Please input YES or NO and hit ENTER.")
                truncate_input = input() #allow for user input
                if truncate_input.lower() == 'yes': #if user types YES or yes, for example, it will still work
                    self._truncate_tables() #truncate tables
                    self.build_dbs() #build Database
                    print('Tables built successfully!')
                else:
                    print("The tables remain populated, and will not be overwritten!")

        self.connection.commit() #commit changes
        self._close_cursor() #close cursor

    def _commit(self):
        '''
        Commit changes made to .db object
        '''
        self.connection.commit() #commit changes
