import sqlite3, csv
import pandas as pd

class Ds_babel:
    def __init__(self, db_name, csv_file):
        self.conn = sqlite3.connect(db_name)
        self.execute = self.conn.cursor().execute
        self.csv = csv_file

    
    def create_table(self, table_name):
        self.execute('''CREATE TABLE {0}(
            Volcano Name Varchar(100),
            Country Varchar(100),
            Type Varchar(100),
            Latitude REAL,
            Longitude REAL,
            Elevation INT
        );'''.format(table_name))

    def read_csv(self):
        with open(self.csv) as csv:
            return csv.readlines()[1:]

    def insert_data(self):
        data = self.read_csv()
        for row in data:
            row = row.split(',')
            if len(row) != 6:
                continue
            row[-3:-1] = map(float, row[-3:-1])
            row[-1] = int(row[-1])

            self.execute('''INSERT INTO {0} VALUES(?,?,?,?,?,?);'''.format(table_name), row)
            self.conn.commit()

    # PART 1   SQL to CSV
    def convert_to_csv(self, table_name):
        data = self.execute('''select * from {0}'''.format(table_name))
        cols = ','.join(desc[0] for desc in data.description)
        data = '\n'.join(','.join(map(str, row)) for row in data.fetchall())
        data = cols +'\n'+ data

        with open(self.csv, 'w') as f:
            f.write(data)

    # PANDAS METHOD 
    # def sql_to_csv(db_name):
    #     conn = sqlite3.connect(db_name)
    #     df = pd.read_sql_query("SELECT * FROM fault_lines", conn)
    #     csv = df.to_csv('all_fault_lines.csv',index=True)

    # PART 2   CSV to SQL
    def convert_to_db(self, table_name):
        self.create_table(table_name)
        self.insert_data(table_name)

    # PANDAS METHOD 
    # def csv_to_db(csv_file):
    # conn = sqlite3.connect('list_volcano.db')
    # df = pd.read_csv(csv_file)
    # df.to_sql('volcanos', conn, if_exists='replace', index=False)

    @classmethod
    def db2csv(cls,db_name):
        csv_file = 'all_fault_lines.csv'
        return cls(db_name, csv_file).convert_to_csv(table_name = 'fault_lines')

    @classmethod
    def csv2db(cls,csv_file):
        db_name = 'list_volcanos.db'
        return cls(db_name, csv_file).convert_to_db(table_name = 'volcanos')


csv = 'list_volcano.csv'
db = 'all_fault_line.db'

if __name__ == '__main__':
    # convert database to csv
    Ds_babel.db2csv(db)

    # convert csv to database
    Ds_babel.csv2db(csv)









