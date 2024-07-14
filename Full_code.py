#Census Data Standardization and Analysis Pipeline

#Importing pandas lib
import pandas as pd

#loading the excel
path="D:\GUVI\Project\Census Data Standardization and Analysis Pipeline\census_2011.xlsx"
census = pd.read_excel(path)


#Task 1: Rename the Column names


#renaming_column

rename_column= {
"State name" : "State/UT",
"District name"  : "District",
"Male_Literate" : "Literate_Male",
"Female_Literate" : "Literate_Female",
"Rural_Households"  : "Households_Rural",
"Urban_Households"  : "Households_Urban",
"Age_Group_0_29" : "Young_and_Adult",
"Age_Group_30_49" : "Middle_Aged",
"Age_Group_50" : "Senior_Citizen",
"Age not stated" : "Age_Not_Stated"
}

census.rename(columns=rename_column, inplace=True)

#Printing the renamed columns

for renamed_col in census:
    print(renamed_col)




#Task 2: Rename State/UT Names

# Function to convert the 1st letter caps then using 'and' in lower case
def uni_case(value):
    new = value.split()
    changed_val = []
    
    for word in new:
        if word.lower() == 'and':
            changed_val.append('and')
        else:
            changed_val.append(word.capitalize())
            
    return ' '.join(changed_val)

# Applying uni_case function to the 'State/UT' column in the DataFrame
census['State/UT'] = census['State/UT'].apply(uni_case)

print(census['State/UT'].head(10))




#Task 3: New State/UT formation


#Opening the text file that has the list of district name of Telangana state
with open(r"D:\GUVI\Project\Census Data Standardization and Analysis Pipeline\Telangana.txt",'r') as file:
    districts_tel = file.read().splitlines() 

print(districts_tel)

#State name changed to Telangana for the listed district
census.loc[census['District'].isin(districts_tel), 'State/UT'] = 'Telangana'


#Changing state name to ladakh for the district Leh and Kargil
census.loc[census['District'].isin(['Leh','Kargil']), 'State/UT'] = 'Ladakh'

#Lists after the changes made 
census[census["State/UT"].isin(["Telangana","Ladakh"])]



#Task 4: Find and process Missing Data

#Missing column in percentage
missing_data_per = census.isnull().sum()/len(census)
for column, percentage in missing_data_per.items():
    print(f'{column} : {percentage:.2%}')

#To fill missing data

#Population = Male + Female
 
to_fill_Population = (census['Male'].notnull() & 
                      census['Population'].isnull() &  
                      census['Female'].notnull())

census.loc[to_fill_Population, 'Population'] = census['Male'] + census['Female']


to_fill_Male = (census['Female'].notnull() & 
                census['Population'].notnull() &  
                census['Male'].isnull())

census.loc[to_fill_Male, 'Male'] = census['Population'] - census['Female'] 

to_fill_Female = (census['Male'].notnull() & 
                  census['Population'].notnull() &  
                  census['Female'].isnull())

census.loc[to_fill_Female, 'Female'] = census['Population'] - census['Male']

#Literate = Literate_Male + Literate_Female

to_fill_Literate = (census['Literate_Male'].notnull() & 
                    census['Literate'].isnull() &  
                    census['Literate_Female'].notnull())

census.loc[to_fill_Literate, 'Literate'] = census['Literate_Male'] + census['Literate_Female']


to_fill_Literate_Male = (census['Literate_Female'].notnull() & 
                         census['Literate'].notnull() &  
                         census['Literate_Male'].isnull())

census.loc[to_fill_Literate_Male, 'Literate_Male'] = census['Literate'] - census['Literate_Female'] 

to_fill_Literate_Female = (census['Literate_Male'].notnull() & 
                           census['Literate'].notnull() &  
                           census['Literate_Female'].isnull())

census.loc[to_fill_Literate_Female, 'Literate_Female'] = census['Literate'] - census['Literate_Male']

#SC = Male_SC + Female_SC

to_fill_SC = (census['Male_SC'].notnull() & 
              census['SC'].isnull() &  
              census['Female_SC'].notnull())

census.loc[to_fill_SC, 'SC'] = census['Male_SC'] + census['Female_SC']


to_fill_Male_SC = (census['Female_SC'].notnull() & 
                   census['SC'].notnull() &  
                   census['Male_SC'].isnull())

census.loc[to_fill_Male_SC, 'Male_SC'] = census['SC'] - census['Female_SC'] 

to_fill_Female_SC = (census['Male_SC'].notnull() & 
                     census['SC'].notnull() &  
                     census['Female_SC'].isnull())

census.loc[to_fill_Female_SC, 'Female_SC'] = census['SC'] - census['Male_SC']

#ST = Male_ST + Female_ST

to_fill_ST = (census['Male_ST'].notnull() & 
              census['ST'].isnull() &  
              census['Female_ST'].notnull())

census.loc[to_fill_ST, 'ST'] = census['Male_ST'] + census['Female_ST']

to_fill_Male_ST = (census['Female_ST'].notnull() & 
                   census['ST'].notnull() &  
                   census['Male_ST'].isnull())

census.loc[to_fill_Male_ST, 'Male_ST'] = census['ST'] - census['Female_ST'] 

to_fill_Female_ST = (census['Male_ST'].notnull() & 
                     census['ST'].notnull() &  
                     census['Female_ST'].isnull())

census.loc[to_fill_Female_ST, 'Female_ST'] = census['ST'] - census['Male_ST']

#Workers = Male_Workers + Female_Workers

to_fill_Workers = (census['Male_Workers'].notnull() & 
                   census['Workers'].isnull() &  
                   census['Female_Workers'].notnull())

census.loc[to_fill_Workers, 'Workers'] = census['Male_Workers'] + census['Female_Workers']

to_fill_Male_Workers = (census['Female_Workers'].notnull() & 
                        census['Workers'].notnull() &  
                        census['Male_Workers'].isnull())

census.loc[to_fill_Male_Workers, 'Male_Workers'] = census['Workers'] - census['Female_Workers'] 

to_fill_Female_Workers = (census['Male_Workers'].notnull() & 
                          census['Workers'].notnull() &  
                          census['Female_Workers'].isnull())

census.loc[to_fill_Female_Workers, 'Female_Workers'] = census['Workers'] - census['Male_Workers']

#Population = Main_Workers + Marginal_Workers + Non_Workers

to_fill_Main_Workers = (census['Population'].notnull() & 
                        census['Main_Workers'].isnull() &  
                        census['Marginal_Workers'].notnull() &
                        census['Non_Workers'].notnull())
 
census.loc[to_fill_Main_Workers, 'Main_Workers'] = census['Population'] - census['Marginal_Workers'] - census['Non_Workers']


to_fill_Marginal_Workers = (census['Population'].notnull() & 
                            census['Marginal_Workers'].isnull() &  
                            census['Main_Workers'].notnull() &
                            census['Non_Workers'].notnull())

census.loc[to_fill_Marginal_Workers, 'Marginal_Workers'] = census['Population'] - census['Main_Workers'] - census['Non_Workers']

to_fill_Non_Workers = (census['Population'].notnull() & 
                       census['Marginal_Workers'].notnull() &  
                       census['Non_Workers'].isnull() &
                       census['Main_Workers'].notnull())
   
census.loc[to_fill_Non_Workers, 'Non_Workers'] = census['Population'] - census['Main_Workers'] - census['Marginal_Workers']

#Workers = Cultivator_Workers + Agricultural_Workers + Household_Workers + Other_Workers

to_fill_Cultivator_Workers = (census['Cultivator_Workers'].isnull() &
                              census['Workers'].notnull() & 
                              census['Agricultural_Workers'].notnull() &  
                              census['Household_Workers'].notnull() &
                              census['Other_Workers'].notnull())

census.loc[to_fill_Cultivator_Workers, 'Cultivator_Workers'] = census['Workers'] - census['Agricultural_Workers'] - census['Household_Workers'] - census['Other_Workers']


to_fill_Agricultural_Workers = (census['Agricultural_Workers'].isnull() &
                                census['Workers'].notnull() & 
                                census['Cultivator_Workers'].notnull() &  
                                census['Household_Workers'].notnull() &
                                census['Other_Workers'].notnull())

census.loc[to_fill_Agricultural_Workers, 'Agricultural_Workers'] = census['Workers'] - census['Cultivator_Workers'] - census['Household_Workers'] - census['Other_Workers']

to_fill_Household_Workers = (census['Household_Workers'].isnull() &
                             census['Workers'].notnull() & 
                             census['Agricultural_Workers'].notnull() &  
                             census['Cultivator_Workers'].notnull() &
                             census['Other_Workers'].notnull())

census.loc[to_fill_Household_Workers, 'Household_Workers'] = census['Workers'] - census['Agricultural_Workers'] - census['Cultivator_Workers'] - census['Other_Workers']

to_fill_Other_Workers = (census['Other_Workers'].isnull() &
                         census['Workers'].notnull() & 
                         census['Agricultural_Workers'].notnull() &  
                         census['Household_Workers'].notnull() &
                         census['Cultivator_Workers'].notnull())

census.loc[to_fill_Other_Workers, 'Other_Workers'] = census['Workers'] - census['Agricultural_Workers'] - census['Household_Workers'] - census['Cultivator_Workers']

#Population = Hindus + Muslims + Christians + Sikhs + Buddhists + Jains + Others_Religions + Religion_Not_Stated


to_fill_Hindus = (census['Hindus'].isnull() &
                  census['Population'].notnull() & 
                  census['Muslims'].notnull() &  
                  census['Christians'].notnull() &
                  census['Sikhs'].notnull() &
                  census['Buddhists'].notnull() &
                  census['Jains'].notnull() &
                  census['Others_Religions'].notnull() &
                  census['Religion_Not_Stated'].notnull())

census.loc[to_fill_Hindus, 'Hindus'] = census['Population'] - census['Muslims'] - census['Christians'] - census['Sikhs'] - census['Buddhists'] - census['Jains'] - census['Others_Religions'] - census['Religion_Not_Stated']


to_fill_Muslims = (census['Muslims'].isnull() &
                   census['Population'].notnull() & 
                   census['Hindus'].notnull() &  
                   census['Christians'].notnull() &
                   census['Sikhs'].notnull() &
                   census['Buddhists'].notnull() &
                   census['Jains'].notnull() &
                   census['Others_Religions'].notnull() &
                   census['Religion_Not_Stated'].notnull())

census.loc[to_fill_Muslims, 'Muslims'] = census['Population'] - census['Hindus'] - census['Christians'] - census['Sikhs'] - census['Buddhists'] - census['Jains'] - census['Others_Religions'] - census['Religion_Not_Stated']

to_fill_Christians = (census['Christians'].isnull() &
                      census['Population'].notnull() & 
                      census['Muslims'].notnull() &  
                      census['Hindus'].notnull() &
                      census['Sikhs'].notnull() &
                      census['Buddhists'].notnull() &
                      census['Jains'].notnull() &
                      census['Others_Religions'].notnull() &
                      census['Religion_Not_Stated'].notnull())

census.loc[to_fill_Christians, 'Christians'] = census['Population'] - census['Muslims'] - census['Hindus'] - census['Sikhs'] - census['Buddhists'] - census['Jains'] - census['Others_Religions'] - census['Religion_Not_Stated']

to_fill_Sikhs = (census['Sikhs'].isnull() &
                 census['Population'].notnull() & 
                 census['Muslims'].notnull() &  
                 census['Christians'].notnull() &
                 census['Hindus'].notnull()&
                 census['Buddhists'].notnull() &
                 census['Jains'].notnull() &
                 census['Others_Religions'].notnull() &
                 census['Religion_Not_Stated'].notnull())

census.loc[to_fill_Sikhs, 'Sikhs'] = census['Population'] - census['Muslims'] - census['Christians'] - census['Hindus'] - census['Buddhists'] - census['Jains'] - census['Others_Religions'] - census['Religion_Not_Stated']

to_fill_Buddhists = (census['Buddhists'].isnull() &
                     census['Population'].notnull() & 
                     census['Muslims'].notnull() &  
                     census['Christians'].notnull() &
                     census['Hindus'].notnull()&
                     census['Sikhs'].notnull() &
                     census['Jains'].notnull() &
                     census['Others_Religions'].notnull() &
                     census['Religion_Not_Stated'].notnull())

census.loc[to_fill_Buddhists, 'Buddhists'] = census['Population'] - census['Muslims'] - census['Christians'] - census['Hindus'] - census['Sikhs'] - census['Jains'] - census['Others_Religions'] - census['Religion_Not_Stated']

to_fill_Jains = (census['Jains'].isnull() &
                 census['Population'].notnull() & 
                 census['Muslims'].notnull() &  
                 census['Christians'].notnull() &
                 census['Hindus'].notnull()&
                 census['Sikhs'].notnull() &
                 census['Buddhists'].notnull() &
                 census['Others_Religions'].notnull() &
                 census['Religion_Not_Stated'].notnull())

census.loc[to_fill_Jains, 'Jains'] = census['Population'] - census['Muslims'] - census['Christians'] - census['Hindus'] - census['Sikhs'] - census['Buddhists'] - census['Others_Religions'] - census['Religion_Not_Stated']

to_fill_Others_Religions = (census['Others_Religions'].isnull() &
                            census['Population'].notnull() & 
                            census['Muslims'].notnull() &  
                            census['Christians'].notnull() &
                            census['Hindus'].notnull()&
                            census['Sikhs'].notnull() &
                            census['Buddhists'].notnull() &
                            census['Jains'].notnull() &
                            census['Religion_Not_Stated'].notnull())

census.loc[to_fill_Others_Religions, 'Others_Religions'] = census['Population'] - census['Muslims'] - census['Christians'] - census['Hindus'] - census['Sikhs'] - census['Buddhists'] - census['Jains'] - census['Religion_Not_Stated']

to_fill_Religion_Not_Stated = (census['Religion_Not_Stated'].isnull() &
                               census['Population'].notnull() & 
                               census['Muslims'].notnull() &  
                               census['Christians'].notnull() &
                               census['Hindus'].notnull()&
                               census['Sikhs'].notnull() &
                               census['Buddhists'].notnull() &
                               census['Jains'].notnull() &
                               census['Others_Religions'].notnull())

census.loc[to_fill_Religion_Not_Stated, 'Religion_Not_Stated'] = census['Population'] - census['Muslims'] - census['Christians'] - census['Hindus'] - census['Sikhs'] - census['Buddhists'] - census['Jains'] - census['Others_Religions']

#Households = Households_Rural + Households_Urban

to_fill_Households = (census['Households_Rural'].notnull() & 
                      census['Households'].isnull() &  
                      census['Households_Urban'].notnull())

census.loc[to_fill_Households, 'Households'] = census['Households_Rural'] + census['Households_Urban']

to_fill_Households_Rural = (census['Households_Urban'].notnull() & 
                      census['Households'].notnull() &  
                      census['Households_Rural'].isnull())

census.loc[to_fill_Households_Rural, 'Households_Rural'] = census['Households'] - census['Households_Urban'] 

to_fill_Households_Urban = (census['Households_Rural'].notnull() & 
                      census['Households'].notnull() &  
                      census['Households_Urban'].isnull())

census.loc[to_fill_Households_Urban, 'Households_Urban'] = census['Households'] - census['Households_Rural'] 

#Literate_Education = Below_Primary_Education + Primary_Education + Middle_Education + Secondary_Education + Higher_Education + Graduate_Education + Other_Education


to_fill_Below_Primary_Education = (census['Below_Primary_Education'].isnull() &
                                   census['Literate_Education'].notnull() & 
                                   census['Primary_Education'].notnull() &  
                                   census['Middle_Education'].notnull() &
                                   census['Secondary_Education'].notnull() &
                                   census['Higher_Education'].notnull() &
                                   census['Graduate_Education'].notnull() &
                                   census['Other_Education'].notnull())                  

census.loc[to_fill_Below_Primary_Education, 'Below_Primary_Education'] = census['Literate_Education'] - census['Primary_Education'] - census['Middle_Education'] - census['Secondary_Education'] - census['Higher_Education'] - census['Graduate_Education']

to_fill_Primary_Education = (census['Primary_Education'].isnull() &
                             census['Literate_Education'].notnull() & 
                             census['Below_Primary_Education'].notnull() &  
                             census['Middle_Education'].notnull() &
                             census['Secondary_Education'].notnull() &
                             census['Higher_Education'].notnull() &
                             census['Graduate_Education'].notnull() &
                             census['Other_Education'].notnull())

census.loc[to_fill_Primary_Education, 'Primary_Education'] = census['Literate_Education'] - census['Below_Primary_Education'] - census['Middle_Education'] - census['Secondary_Education'] - census['Higher_Education'] - census['Graduate_Education']

to_fill_Middle_Education = (census['Middle_Education'].isnull() &
                            census['Literate_Education'].notnull() & 
                            census['Primary_Education'].notnull() &  
                            census['Below_Primary_Education'].notnull() &
                            census['Secondary_Education'].notnull() &
                            census['Higher_Education'].notnull() &
                            census['Graduate_Education'].notnull() &
                            census['Other_Education'].notnull())

census.loc[to_fill_Middle_Education, 'Middle_Education'] = census['Literate_Education'] - census['Primary_Education'] - census['Below_Primary_Education'] - census['Secondary_Education'] - census['Higher_Education'] - census['Graduate_Education'] - census['Other_Education']

to_fill_Secondary_Education = (census['Secondary_Education'].isnull() &
                               census['Literate_Education'].notnull() & 
                               census['Primary_Education'].notnull() &  
                               census['Middle_Education'].notnull() &
                               census['Below_Primary_Education'].notnull()&
                               census['Higher_Education'].notnull() &
                               census['Graduate_Education'].notnull() &
                               census['Other_Education'].notnull())

census.loc[to_fill_Secondary_Education, 'Secondary_Education'] = census['Literate_Education'] - census['Primary_Education'] - census['Middle_Education'] - census['Below_Primary_Education'] - census['Higher_Education'] - census['Graduate_Education'] - census['Other_Education']

to_fill_Higher_Education = (census['Higher_Education'].isnull() &
                            census['Literate_Education'].notnull() & 
                            census['Primary_Education'].notnull() &  
                            census['Middle_Education'].notnull() &
                            census['Below_Primary_Education'].notnull()&
                            census['Secondary_Education'].notnull() &
                            census['Graduate_Education'].notnull() &
                            census['Other_Education'].notnull())

census.loc[to_fill_Higher_Education, 'Higher_Education'] = census['Literate_Education'] - census['Primary_Education'] - census['Middle_Education'] - census['Below_Primary_Education'] - census['Secondary_Education'] - census['Graduate_Education'] - census['Other_Education'] - census['Religion_Not_Stated']

to_fill_Graduate_Education = (census['Graduate_Education'].isnull() &
                              census['Literate_Education'].notnull() & 
                              census['Primary_Education'].notnull() &  
                              census['Middle_Education'].notnull() &
                              census['Below_Primary_Education'].notnull()&
                              census['Secondary_Education'].notnull() &
                              census['Higher_Education'].notnull() &
                              census['Other_Education'].notnull())

census.loc[to_fill_Graduate_Education, 'Graduate_Education'] = census['Literate_Education'] - census['Primary_Education'] - census['Middle_Education'] - census['Below_Primary_Education'] - census['Secondary_Education'] - census['Higher_Education'] - census['Other_Education']

to_fill_Other_Education = (census['Other_Education'].isnull() &
                           census['Literate_Education'].notnull() & 
                           census['Primary_Education'].notnull() &  
                           census['Middle_Education'].notnull() &
                           census['Below_Primary_Education'].notnull()&
                           census['Secondary_Education'].notnull() &
                           census['Higher_Education'].notnull() &
                           census['Graduate_Education'].notnull())

census.loc[to_fill_Other_Education, 'Other_Education'] = census['Literate_Education'] - census['Primary_Education'] - census['Middle_Education'] - census['Below_Primary_Education'] - census['Secondary_Education'] - census['Higher_Education'] - census['Graduate_Education'] 



#Total_Education = Literate_Education + Illiterate_Education

to_fill_Total_Education = (census['Literate_Education'].notnull() & 
                           census['Total_Education'].isnull() &  
                           census['Illiterate_Education'].notnull())

census.loc[to_fill_Total_Education, 'Total_Education'] = census['Literate_Education'] + census['Illiterate_Education']

to_fill_Literate_Education = (census['Illiterate_Education'].notnull() & 
                              census['Total_Education'].notnull() &  
                              census['Literate_Education'].isnull())

census.loc[to_fill_Literate_Education, 'Literate_Education'] = census['Total_Education'] - census['Illiterate_Education'] 

to_fill_Illiterate_Education = (census['Literate_Education'].notnull() & 
                                census['Total_Education'].notnull() &  
                                census['Illiterate_Education'].isnull())

census.loc[to_fill_Illiterate_Education, 'Illiterate_Education'] = census['Total_Education'] - census['Literate_Education']


#Population  = Young_and_Adult+  Middle_Aged + Senior_Citizen + Age_Not_Stated

to_fill_Young_and_Adult = (census['Young_and_Adult'].isnull() &
                           census['Population'].notnull() & 
                           census['Middle_Aged'].notnull() &  
                           census['Senior_Citizen'].notnull() &
                           census['Age_Not_Stated'].notnull())

census.loc[to_fill_Young_and_Adult, 'Young_and_Adult'] = census['Population'] - census['Middle_Aged'] - census['Senior_Citizen'] - census['Age_Not_Stated']

to_fill_Middle_Aged = (census['Middle_Aged'].isnull() &
                       census['Population'].notnull() & 
                       census['Young_and_Adult'].notnull() &  
                       census['Senior_Citizen'].notnull() &
                       census['Age_Not_Stated'].notnull())

census.loc[to_fill_Middle_Aged, 'Middle_Aged'] = census['Population'] - census['Young_and_Adult'] - census['Senior_Citizen'] - census['Age_Not_Stated']

to_fill_Senior_Citizen = (census['Senior_Citizen'].isnull() &
                          census['Population'].notnull() & 
                          census['Middle_Aged'].notnull() &  
                          census['Young_and_Adult'].notnull() &
                          census['Age_Not_Stated'].notnull())

census.loc[to_fill_Senior_Citizen, 'Senior_Citizen'] = census['Population'] - census['Middle_Aged'] - census['Young_and_Adult'] - census['Age_Not_Stated']

to_fill_Age_Not_Stated = (census['Age_Not_Stated'].isnull() &
                          census['Population'].notnull() & 
                          census['Middle_Aged'].notnull() &  
                          census['Senior_Citizen'].notnull() &
                          census['Young_and_Adult'].notnull())

census.loc[to_fill_Age_Not_Stated, 'Age_Not_Stated'] = census['Population'] - census['Middle_Aged'] - census['Senior_Citizen'] - census['Young_and_Adult']

#Task  5: Save Data to MongoDB

from pymongo import MongoClient

# Connecting to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['testdb']
collection = db['census']

# Converting DataFrame to dictionary format
data_dict = census.to_dict("records")

# Inserting data into MongoDB
collection.insert_many(data_dict)

# opening the collection
for data in collection.find():
    print(data)
    
    


from sqlalchemy import create_engine #, Column, Integer, String, Float  # Adjust data types as needed
#from pandas import DataFrame


mongo_data = list(collection.find())

# Remove the MongoDB ObjectId
for record in mongo_data:
    record.pop('_id', None)


# coverting monggodb data to dataframe
census_mtos = pd.DataFrame(mongo_data)

#Defining function to list out the lengthy column names
def print_lengthy_columns(census_mtos, max_length=30):
    lengthy_columns = [col for col in census.columns if len(col) > max_length]

    if lengthy_columns:
        for col in lengthy_columns:
            print(f"- {col}")
    else:
        print("No lengthy columns found.")

print_lengthy_columns(census_mtos)


#renaming column name for the lengthy columns

rename_lengthy_column= {
"Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households" : "Households_Night_Soil_disposed into_Open_Drain",
"Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households"  : "Households_Flush_Latrine_connect_to_Other_System",
"Not_having_bathing_facility_within_the_premises_Total_Households" : "Households_Without_Bathing_Facility",
"Not_having_latrine_facility_within_the_premises_Alternative_source_Open_Households" : "Households_Without_Latrine_facility",
"Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households"  : "Households_Handpump_Tubewell_Borewell_Water",
"Main_source_of_drinking_water_Other_sources_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households"  : "Households_Natural_Water_Sources",
"Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car": "Households_Comprehensive_Electronics_Vehicles"
}

census_mtos.rename(columns=rename_lengthy_column, inplace=True)

for col in census_mtos:
    print(col)

#MySQL Database credentials
username = 'root'
password = 'Reyno#2431'
host = 'localhost'
port = 3306
database = 'test'

# Create the database URL
database_url = f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}'

# Create the engine
engine = create_engine(database_url)

#Loading data to MySQL
census_mtos.to_sql('census_mysql', con=engine, if_exists='append', index=False)

