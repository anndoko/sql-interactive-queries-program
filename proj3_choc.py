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


# Part 3: Implement interactive prompt. We've started for you!
# functions for the interactive part
# --- bars ---
def bars_query(specification="", keyword="", criteria="ratings", sorting_order="top", limit=10):
    # connect db
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()


    # form the statement
    if "c1" in specification:
        statement = "SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin, c1.Alpha2 "
        statement += "FROM Bars "
        statement += "JOIN Countries AS c1 ON Bars.CompanyLocationId = c1.Id "
    elif "c2" in specification:
        statement = "SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin, c2.Alpha2 "
        statement += "FROM Bars "
        statement += "JOIN Countries AS c2 ON Bars.BroadBeanOriginId = c2.Id "
    else:
        statement = "SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin "
        statement += "FROM Bars "

    # specifications
    if specification != "":
        if "Alpha2" in specification:
            keyword = keyword.upper()
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
    if sorting_order == "top":
        statement += "{} ".format("DESC")
    elif sorting_order == "bottom":
        statement += "{} ".format("ASC")

    # limit
    statement += "LIMIT {}".format(limit) #list the top <limit> matches or the bottom <limit> matches.
    print(statement)

    # excute the statement
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results

# --- companies ---
def companies_query(specification="", keyword="", criteria="ratings", sorting_order="top", limit=10):
    # connect db
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # form the statement

    # ratings / cocoa
    if criteria == "ratings":
        statement = "SELECT Company, CompanyLocation, AVG(Rating) "
    elif criteria == "cocoa":
        statement = "SELECT Company, CompanyLocation, AVG(CocoaPercent), COUNT(SpecificBeanBarName) "
    elif criteria == "bars_sold":
        statement = "SELECT Company, CompanyLocation, COUNT(SpecificBeanBarName)"

    statement += "FROM Bars "

    # form the statement
    if "c1.Alpha2" in specification:
        statement += "JOIN Countries AS c1 ON Bars.CompanyLocationId = c1.Id "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    elif "c2.Alpha2" in specification:
        statement += "JOIN Countries AS c2 ON Bars.BroadBeanOriginId = c2.Id "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    elif specification == "Alpha2" or specification == "Region":
        statement += "JOIN Countries ON Bars.CompanyLocation = Countries.EnglishName "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    else:
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "



    # specifications
    if specification != "":
        if "Alpha2" in specification:
            keyword = keyword.upper()
        try:
            statement += "AND {} = '{}' ".format(specification , keyword)
        except:
            print("Failure. Please try again.")



    # ratings / cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("AVG(Rating)")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("AVG(CocoaPercent)")
    elif criteria == "bars_sold":
        statement += "ORDER BY {} ".format("COUNT(SpecificBeanBarName)")

    # top: DESC / bottom ASC
    if sorting_order == "top":
        statement += "{} ".format("DESC")
    elif sorting_order == "bottom":
        statement += "{} ".format("ASC")

    # limit
    statement += "LIMIT {}".format(limit) #list the top <limit> matches or the bottom <limit> matches.
    print(statement)

    # excute the statement
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results



# --- countries ---
def countries_query(specification="", keyword="", criteria="ratings", sorting_order="top", limit=10):
    # connect db
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # form the statement
    statement = "SELECT Company, CompanyLocation, AVG(Rating), AVG(CocoaPercent), COUNT(SpecificBeanBarName), Alpha2 "
    statement += "FROM Bars "
    statement += "JOIN Countries ON Bars.CompanyLocationId = Countries.Id "
    statement += "GROUP BY Company "
    statement += "HAVING COUNT(SpecificBeanBarName) >= 4 "

    # specifications
    if specification != "":
        try:
            statement += "AND {} = '{}' ".format(specification , keyword.upper())
        except:
            print("Failure. Please try again.")

    # ratings / cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("Rating")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("CocoaPercent")
    elif criteria == "bars_sold":
        statement += "ORDER BY {} ".format("COUNT(SpecificBeanBarName)")

    # top: DESC / bottom ASC
    if sorting_order == "top":
        statement += "{} ".format("DESC")
    elif sorting_order == "bottom":
        statement += "{} ".format("ASC")

    # excute the statement
    result_lst = []
    rows = cur.execute(statement)
    for row in rows:
        result_lst.append(row)
    pass

# --- regions ---
def regions_query(arg):
    # connect db
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()


    # ratings / cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("Rating")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("CocoaPercent")
    elif criteria == "bars_sold":
        statement += "ORDER BY {} ".format("COUNT(SpecificBeanBarName)")

    # top: DESC / bottom ASC
    if sorting_order == "top":
        statement += "{} ".format("DESC")
    elif sorting_order == "bottom":
        statement += "{} ".format("ASC")

    # excute the statement
    rows = cur.execute(statement)
    for row in rows:
        print(row)
    pass

# Part 2: Implement logic to process user commands
def process_command(command):

    command_lst = command.lower().split()

    # def bars_query(specification="", keyword="", criteria="ratings", sorting_order="top", limit=10):
    command_dic = {
        "specification": "",
        "keyword": "",
        "criteria": "ratings",
        "sorting_order": "top",
        "limit": 0
    }

    # lists for checing the commend
    query_type_lst = ["bars", "companies", "countries", "regions"]
    sorting_criteria_lst = ["cocoa", "ratings", "bars_sold"]
    sorting_order_lst = ["top", "bottom"]
    specification_lst = ["sellcountry", "sourcecountry", "sellregion", "sourceregion", "country", "region"]

    # check user's command line
    for command in command_lst:
        # query type
        if command in query_type_lst:
            command_dic["query_type"] = command
        # sorting criteria
        elif command in sorting_criteria_lst:
            command_dic["criteria"] = command
        # number of matches & specifications
        elif "=" in command:
            lst = command.split("=")
            for ele in lst:
                # top/bottom & limit
                if ele in sorting_order_lst:
                    command_dic["sorting_order"] = lst[0]
                    command_dic["limit"] = lst[1]
                # specifications
                elif ele in specification_lst:
                    if lst[0] == "sellcountry":
                        command_dic["specification"] = "c1.Alpha2"
                    elif lst[0] == "sourcecountry":
                        command_dic["specification"] = "c2.Alpha2"
                    elif lst[0] == "sellregion":
                        command_dic["specification"] = "c1.Region"
                    elif lst[0] == "sourceregion":
                        command_dic["specification"] = "c2.Region"
                    elif lst[0] == "country":
                        command_dic["specification"] = "Alpha2"
                    else:
                        command_dic["specification"] = lst[0].title()
                    command_dic["keyword"] = lst[1].title()

    # execute bars_query
    results = []

    if command_dic["query_type"] == "bars":
        results = bars_query(command_dic["specification"], command_dic["keyword"], command_dic["criteria"], command_dic["sorting_order"], command_dic["limit"])
    elif command_dic["query_type"] == "companies":
        results = companies_query(command_dic["specification"], command_dic["keyword"], command_dic["criteria"], command_dic["sorting_order"], command_dic["limit"])

    return results




# 95
result_95 = process_command('companies cocoa top=5')
print("result 95:", result_95[0][0])


def load_help_text():
    with open('help.txt') as f:
        return f.read()

def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    init_db_tables()
    read_csv_file_and_insert_data(BARSCSV)
    read_json_file_and_insert_data(COUNTRIESJSON)
    update_tables()

    interactive_prompt()
