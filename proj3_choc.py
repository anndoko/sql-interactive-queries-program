import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def init_db_tables():
    # Create db
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Failure. Please try again.")

    # Drop tables if they exist
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)
    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)
    conn.commit()

    # -- Create tables: Bars --
    statement = '''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' TEXT NOT NULL,
            'REF' TEXT,
            'ReviewDate' TEXT,
            'CocoaPercent' REAL,
            'CompanyLocation' TEXT,
            'CompanyLocationId' INTEGER,
            'Rating' REAL,
            'BeanType' TEXT,
            'BroadBeanOrigin' TEXT,
            'BroadBeanOriginId' INTEGER
        );
    '''
    try:
        cur.execute(statement)
        print("Execute the statement to create the table: Bars")
    except:
        print("Failure. Please try again.")
    conn.commit()

    # -- Create tables: Countries --
    statement = '''
        CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT,
            'Alpha3' TEXT,
            'EnglishName' TEXT,
            'Region' TEXT,
            'Subregion' TEXT,
            'Population' INTEGER,
            'Area' REAL
        );
    '''
    try:
        cur.execute(statement)
        print("Execute the statement to create the table: Countries")
    except:
        print("Failure. Please try again.")
    conn.commit()

def read_csv_file_and_insert_data(FILENAME):
    # read data from CSV
    with open(FILENAME, 'r') as csv_f:
        csv_data = csv.reader(csv_f)

        # This skips the first row of the CSV file.
        # csvreader.next() also works in Python 2.
        next(csv_data)

        for row in csv_data:
            (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocation, Rating, BeanType, BroadBeanOrigin) = row

            CocoaPercent = float(CocoaPercent.strip('%'))

            try:
                conn = sqlite3.connect(DBNAME)
                cur = conn.cursor()
            except:
                print("Failure. Please try again.")

            insert_statement = '''
                INSERT INTO Bars(Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocation, Rating, BeanType, BroadBeanOrigin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            '''

            # execute and commit
            cur.execute(insert_statement, [Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocation, Rating, BeanType, BroadBeanOrigin])
            conn.commit()

def read_json_file_and_insert_data(FILENAME):
    # read data from JSON
    json_f = open(FILENAME, 'r')
    json_f_content = json_f.read()
    json_data = json.loads(json_f_content)

    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Failure. Please try again.")

    for row in json_data:
        Alpha2 = row["alpha2Code"]
        Alpha3 = row["alpha3Code"]
        EnglishName = row["name"]
        Region = row["region"]
        Subregion = row["subregion"]
        Population = row["population"]
        Area = row["area"]

        insert_statement = '''
            INSERT INTO Countries(Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area) VALUES (?, ?, ?, ?, ?, ?, ?);
        '''

        # execute and commit
        cur.execute(insert_statement, [Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area])
        conn.commit()

def update_tables():
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Failure. Please try again.")

    # set CompanyLocationId &
    update_CompanyLocationId = '''
        UPDATE Bars
        SET (CompanyLocationId) = (SELECT c.ID FROM Countries c WHERE Bars.CompanyLocation = c.EnglishName)
    '''

    update_BroadBeanOriginId = '''
        UPDATE Bars
        SET (BroadBeanOriginId) = (SELECT c.ID FROM Countries c WHERE Bars.BroadBeanOrigin = c.EnglishName)
    '''

    # execute and commit
    cur.execute(update_CompanyLocationId)
    cur.execute(update_BroadBeanOriginId)
    conn.commit()

init_db_tables()
read_csv_file_and_insert_data(BARSCSV)
read_json_file_and_insert_data(COUNTRIESJSON)
update_tables()

# Part 2: Implement logic to process user commands
def process_command(command):
    return []


def load_help_text():
    with open('help.txt') as f:
        return f.read()


# Part 3: Implement interactive prompt. We've started for you!

# functions for the interactive part
# --- bars ---
# 	Options:
# 		* sellcountry=<name>|sourcecountry=<name>|
# 			sellregion=<name>|sourceregion=<geo_name> [default: none]
# 		Description: Specifies a country or region within which to limit the
# 		results, and also specifies whether to limit by the seller
# 		(or manufacturer) or by the bean origin source.

def bars_query(specification="", keyword="", criteria="ratings", type="top", limit=10):
    # connect db
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # form the statement
    statement = "SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin "
    statement += "FROM Bars "

    # specifications
    if specification != "":
        try:
            statement += "WHERE {} = '{}' ".format(specification , keyword)
        except:
            print("Failure. Please try again.")

    # ratings / cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("Rating")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("CocoaPercent")

    # top: DESC / bottom ASC
    if type == "top":
        statement += "{} ".format("DESC")
    elif type == "bottom":
        statement += "{} ".format("ASC")

    # limit
    statement += "LIMIT {}".format(limit) #list the top <limit> matches or the bottom <limit> matches.

    # excute the statement
    rows = cur.execute(statement)
    for row in rows:
        print(row)
    conn.commit()

bars_ratings(specification="Company", keyword="Luker", criteria="ratings", type="top", limit=10)
# --- companies ---

# --- countries ---

# --- regions ---


def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue

# Make sure nothing runs or prints out when this file is run as a module
# if __name__=="__main__":
#     interactive_prompt()
