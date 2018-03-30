import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Read data from CSV and JSON into a new database called choc.db
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

# Queries
# --- bars ---
def bars_query(specification="", keyword="", criteria="ratings", sorting_order="top", limit="10"):
    # connect db
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # form the statement
    if "c1" in specification:
        statement = "SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin "
        statement += "FROM Bars "
        statement += "JOIN Countries AS c1 ON Bars.CompanyLocationId = c1.Id "
    elif "c2" in specification:
        statement = "SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin "
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

    # excute the statement
    # print(statement)
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results

# --- companies ---
def companies_query(specification="", keyword="", criteria="ratings", sorting_order="top", limit="10"):
    # connect db
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # form the statement
    if criteria == "ratings":
        statement = "SELECT Company, CompanyLocation, AVG(Rating) "
    elif criteria == "cocoa":
        statement = "SELECT Company, CompanyLocation, AVG(CocoaPercent) "
    elif criteria == "bars_sold":
        statement = "SELECT Company, CompanyLocation, COUNT(SpecificBeanBarName) "

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

    # excute the statement
    # print(statement)
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results

# --- countries ---
def countries_query(specification="", keyword="", criteria="ratings", sorting_order="top", limit="10", sellers_or_sources="sellers"):
    # connect db
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = "SELECT EnglishName, Region, "

    # form the statement
    if criteria == "ratings":
        statement += "AVG(Rating) "
    elif criteria == "cocoa":
        statement += "AVG(CocoaPercent) "
    elif criteria == "bars_sold":
        statement += "COUNT(SpecificBeanBarName) "

    statement += "FROM Countries "

    # form the statement
    if sellers_or_sources == "sellers":
        statement += "JOIN Bars ON Countries.Id = Bars.CompanyLocationId "
    elif sellers_or_sources == "sources":
        statement += "JOIN Bars ON Countries.Id = Bars.BroadBeanOriginId "

    statement += "GROUP BY EnglishName "
    statement += "HAVING COUNT(SpecificBeanBarName) > 4 "

    # specifications
    if specification != "":
        if "Region" in specification:
            keyword = keyword.title()
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

    # excute the statement
    # print(statement)
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results

# --- regions ---
def regions_query(specification="", keyword="", criteria="ratings", sorting_order="top", limit="10", sellers_or_sources="sellers"):
    # connect db
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # form the statement
    statement = "SELECT Region, "

    if criteria == "ratings":
        statement += "AVG(Rating) "
    elif criteria == "cocoa":
        statement += "AVG(CocoaPercent) "
    elif criteria == "bars_sold":
        statement += "COUNT(SpecificBeanBarName) "

    statement += "FROM Countries "

    # form the statement
    if sellers_or_sources == "sellers":
        statement += "JOIN Bars ON Countries.Id = Bars.CompanyLocationId "
    elif sellers_or_sources == "sources":
        statement += "JOIN Bars ON Countries.Id = Bars.BroadBeanOriginId "


    statement += "GROUP BY Region "
    statement += "HAVING COUNT(SpecificBeanBarName) > 4 "

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

    # excute the statement
    # print(statement)
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results

# functions for formatting the output
def str_output(string_output):
    if len(string_output) > 12:
        formatted_output = string_output[:12] + "..."
    else:
        formatted_output = string_output
    return formatted_output

def percent_output(cocoa_float):
    formatted_output = str(cocoa_float).replace(".0", "%")
    return formatted_output

def digits_output(rating_float):
    formatted_output = "{0:.1f}".format(rating_float, 1)
    return formatted_output

# Implement logic to process user commands
def process_command(command):

    command_lst = command.lower().split()

    # def bars_query(specification="", keyword="", criteria="ratings", sorting_order="top", limit="10"):
    command_dic = {
        "specification":"",
        "keyword":"",
        "criteria":"ratings",
        "sorting_order":"top",
        "limit":"10",
        "sellers_or_sources":"sellers"
    }

    # lists for checing the commend
    query_type_lst = ["bars", "companies", "countries", "regions"]
    sorting_criteria_lst = ["cocoa", "ratings", "bars_sold"]
    sorting_order_lst = ["top", "bottom"]
    specification_lst = ["sellcountry", "sourcecountry", "sellregion", "sourceregion", "country", "region", "sellers", "sources"]
    sellers_or_sources_lst = ["sellers", "sources"]

    if_valid = True

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
        elif command in sellers_or_sources_lst:
            command_dic["sellers_or_sources"] = command

    results = []

    if command_dic["query_type"] == "bars" and if_valid == True:
        # execute bars_query
        results = bars_query(command_dic["specification"], command_dic["keyword"], command_dic["criteria"], command_dic["sorting_order"], command_dic["limit"])

        # output
        # 'SpecificBeanBarName','Company', 'CompanyLocation', 'Rating', 'CocoaPercent', 'BroadBeanOrigin'
        template = "{0:20} {1:20} {2:20} {3:20} {4:20} {5:20}"
        for row in results:
            (sbbn, c, cl, r, cp, bbo) = row
            print(template.format(str_output(sbbn), str_output(c), str_output(cl), digits_output(r), percent_output(cp), str_output(bbo)))

        return results

    elif command_dic["query_type"] == "companies" and if_valid == True:
        # execute companies_query
        results = companies_query(command_dic["specification"], command_dic["keyword"], command_dic["criteria"], command_dic["sorting_order"], command_dic["limit"])

        # output
        # 'Company', 'CompanyLocation', <agg> (i.e., average rating or cocoa percent, or number of bars sold)
        template = "{0:20} {1:20} {2:20}"
        for row in results:
            (c, cl, agg) = row

            if command_dic["criteria"] == "ratings":
                agg = digits_output(agg)
            elif command_dic["criteria"] == "cocoa":
                agg = percent_output(agg)

            print(template.format(str_output(c), str_output(cl), agg))

        return results

    elif command_dic["query_type"] == "countries" and if_valid == True:
        # execute countries_query
        results = countries_query(command_dic["specification"], command_dic["keyword"], command_dic["criteria"], command_dic["sorting_order"], command_dic["limit"], command_dic["sellers_or_sources"])

        # output
        # 'Country', 'Region', <agg> (i.e., average rating or cocoa percent, or number of bars sold)
        template = "{0:20} {1:20} {2:20}"
        for row in results:
            (c, r, agg) = row

            if command_dic["criteria"] == "ratings":
                agg = digits_output(agg)
            elif command_dic["criteria"] == "cocoa":
                agg = percent_output(agg)

            print(template.format(str_output(c), str_output(r), agg))

        return results

    elif command_dic["query_type"] == "regions" and if_valid == True:
        # execute regions_query
        results = regions_query(command_dic["specification"], command_dic["keyword"], command_dic["criteria"], command_dic["sorting_order"], command_dic["limit"], command_dic["sellers_or_sources"])

        # output
        # 'Region', <agg> (i.e., average rating or cocoa percent, or number of bars sold)
        template = "{0:15} {1:15}"
        for row in results:
            (r, agg) = row

            if command_dic["criteria"] == "ratings":
                agg = digits_output(agg)
            elif command_dic["criteria"] == "cocoa":
                agg = percent_output(agg)

            print(template.format(str_output(r), agg))

        return results

def load_help_text():
    with open('help.txt') as f:
        return f.read()

def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        try:
            results = process_command(response)
        except:
            continue

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
