import streamlit as st
from sqlalchemy import create_engine
import pandas as pd

# Function to create a connection to the MySQL database
username = 'root'
password = 'Reyno#2431'
host = 'localhost'
port = 3306
database = 'test'


# Create the engine
engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}")


st.header("The Total Population of Each District")
query1 = "SELECT District, Population AS Total_Population FROM census_test order by District"

# Execute the query and fetch the results
result_df = pd.read_sql(query1, engine)

# Display the results
st.write(result_df)

#Literate males and females in each district

st.header("Literate males and females in each district")
query2 = "SELECT District, Literate_Male, Literate_Female FROM census_test order by District"

result_df = pd.read_sql(query2, engine)

st.write(result_df)

#The percentage of workers (both male and female) in each district
st.header("The percentage of workers (both male and female) in each district")
query3 = "SELECT District, ROUND((Workers/Population)*100, 2) AS Worker_Percentage FROM census_test order BY District"

result_df = pd.read_sql(query3, engine)

st.write(result_df)

#Households have access to LPG or PNG as a cooking fuel in each district
st.header("Households have access to LPG or PNG as a cooking fuel in each district")
query4 = "SELECT District, LPG_or_PNG_Households FROM census_test ORDER BY District"

result_df = pd.read_sql(query4, engine)

st.write(result_df)


#Religious composition (Hindus, Muslims, Christians, etc.) of each district?
st.header("Religious composition (Hindus, Muslims, Christians, etc.) of each district")
query5 = "SELECT District, Hindus,Muslims,Christians,Sikhs,Buddhists,Jains,Others_Religions,Religion_Not_Stated \
FROM census_test order BY District;"
result_df = pd.read_sql(query5, engine)

st.write(result_df)

#Households have internet access in each district
st.header("Households have internet access in each district")
query6 = "SELECT District, Households_with_Internet FROM census_test order by District;"

result_df = pd.read_sql(query6, engine)

st.write(result_df)



#the educational attainment distribution (below primary, primary, middle, secondary, etc.) in each distric
st.header("The educational attainment distribution (below primary, primary, middle, secondary, etc.) in each district")
query7 = "SELECT District, \
    Below_Primary_Education, \
        Primary_Education, \
            Middle_Education, \
                Secondary_Education, \
                    Higher_Education, \
                        Graduate_Education, \
                            Other_Education \
                                FROM census_test;"


result_df = pd.read_sql(query7, engine)

st.write(result_df)

#Households have access to various modes of transportation (bicycle, car, radio, television, etc.) in each district
st.header("Households have access to various modes of transportation (bicycle, car, radio, television, etc.) in each district")
query8 = "SELECT District, \
       Households_with_Bicycle, \
       Households_with_Car_Jeep_Van, \
       Households_with_Radio_Transistor, \
       Households_with_Television, \
	   Households_with_Scooter_Motorcycle_Moped \
FROM census_test \
ORDER BY District"


#Condition of occupied census houses (dilapidated, with separate kitchen, with bathing facility, with latrine facility, etc.) in each district
st.header("Condition of occupied census houses (dilapidated, with separate kitchen, with bathing facility, with latrine facility, etc.) in each district")
query9 = "SELECT District, \
Household_size_1_person_Households, \
Household_size_2_persons_Households, \
Household_size_1_to_2_persons, \
Household_size_3_persons_Households, \
Household_size_3_to_5_persons_Households, \
Household_size_4_persons_Households, \
Household_size_5_persons_Households, \
Household_size_6_8_persons_Households, \
Household_size_9_persons_and_above_Households \
FROM census_test ORDER BY District"

result_df = pd.read_sql(query9, engine)

st.write(result_df)

#Household size distributed (1 person, 2 persons, 3-5 persons, etc.) in each district
st.header("Household size distributed (1 person, 2 persons, 3-5 persons, etc.) in each district")
query10 = "SELECT District, \
Household_size_1_person_Households, \
Household_size_2_persons_Households, \
Household_size_1_to_2_persons, \
Household_size_3_persons_Households, \
Household_size_3_to_5_persons_Households, \
Household_size_4_persons_Households, \
Household_size_5_persons_Households, \
Household_size_6_8_persons_Households, \
Household_size_9_persons_and_above_Households \
FROM census_test ORDER BY District"

result_df = pd.read_sql(query10, engine)

st.write(result_df)

#The total number of households in each state
st.header("The total number of households in each state")
query11 ="SELECT `State/UT`, SUM(Households) AS Total_Households FROM census_test GROUP BY `State/UT`"

result_df = pd.read_sql(query11, engine)

st.write(result_df)

#Households have a latrine facility within the premises in each state
st.header("Households have a latrine facility within the premises in each state")
query12 = "SELECT `State/UT`, SUM(Having_latrine_facility_within_the_premises_Total_Households) AS Households_with_Latrine \
    FROM census_test GROUP BY `State/UT`"

result_df = pd.read_sql(query12, engine)

st.write(result_df)

#The average household size in each state
st.header("The average household size in each state")
query13= "SELECT District, \
    AVG(Household_size_1_person_Households+Household_size_2_persons_Households+ \
        Household_size_1_to_2_persons+Household_size_3_persons_Households+ \
            Household_size_3_to_5_persons_Households+Household_size_4_persons_Households+ \
                Household_size_5_persons_Households+Household_size_6_8_persons_Households+ \
                    Household_size_9_persons_and_above_Households) as average_household_size \
FROM census_test GROUP BY District"

result_df = pd.read_sql(query13, engine)

st.write(result_df)


#Households are owned versus rented in each state
st.header("Households are owned versus rented in each state")
query14 ="SELECT `State/UT`,SUM(Ownership_Owned_Households) AS Owned_Households,SUM(Ownership_Rented_Households) AS Rented_Households \
FROM census_test GROUP BY `State/UT`"

result_df = pd.read_sql(query14, engine)

st.write(result_df)

#The distribution of different types of latrine facilities (pit latrine, flush latrine, etc.) in each state
st.header("the distribution of different types of latrine facilities (pit latrine, flush latrine, etc.) in each state")
query15 ="SELECT `State/UT`, \
       SUM(Type_of_latrine_facility_Pit_latrine_Households) AS Pit_latrine, \
       SUM(Type_of_latrine_facility_Other_latrine_Households) AS Other_latrine, \
       SUM(Households_Flush_Latrine_connect_to_Other_System) AS Flush_Latrine \
FROM census_test GROUP BY `State/UT`"

result_df = pd.read_sql(query15, engine)

st.write(result_df)

#Households have access to drinking water sources near the premises in each state
st.header("Households have access to drinking water sources near the premises in each state")
query16 = "SELECT `State/UT`, \
    SUM(Location_of_drinking_water_source_Near_the_premises_Households) AS Households_with_Drinking_Water_Near \
FROM census_test GROUP BY `State/UT`"

result_df = pd.read_sql(query16, engine)

st.write(result_df)

#The average household income distribution in each state based on the power parity categories
st.header("The average household income distribution in each state based on the power parity categories")
query17 = "SELECT `State/UT`, \
       ROUND(AVG(Power_Parity_Above_Rs_545000), 2) AS Average_Income_Above_Rs_545000, \
       ROUND(AVG(Power_Parity_Less_than_Rs_45000), 2) AS Average_Income_Less_than_Rs_45000, \
       ROUND(AVG(Power_Parity_Rs_150000_240000), 2) AS Average_Income_Rs_150000_240000, \
       ROUND(AVG(Power_Parity_Rs_150000_330000), 2) AS Average_Income_Rs_150000_330000, \
       ROUND(AVG(Power_Parity_Rs_240000_330000), 2) AS Average_Income_Rs_240000_330000, \
       ROUND(AVG(Power_Parity_Rs_330000_425000), 2) AS Average_Income_Rs_330000_425000, \
       ROUND(AVG(Power_Parity_Rs_330000_545000), 2) AS Average_Income_Rs_330000_545000, \
       ROUND(AVG(Power_Parity_Rs_425000_545000), 2) AS Average_Income_Rs_425000_545000, \
       ROUND(AVG(Power_Parity_Rs_45000_150000), 2) AS Average_Income_Rs_45000_150000, \
       ROUND(AVG(Power_Parity_Rs_45000_90000), 2) AS Average_Income_Rs_45000_90000, \
       ROUND(AVG(Power_Parity_Rs_90000_150000), 2) AS Average_Income_Rs_90000_150000 \
FROM census_test GROUP BY `State/UT`"

result_df = pd.read_sql(query17, engine)

st.write(result_df)

#The percentage of married couples with different household sizes in each state
st.header("The percentage of married couples with different household sizes in each state")
query18 = "SELECT `State/UT`, \
       ROUND(SUM(Married_couples_1_Households) / SUM(Households) * 100, 2) AS Percentage_Married_Couples_1, \
       ROUND(SUM(Married_couples_2_Households) / SUM(Households) * 100, 2) AS Percentage_Married_Couples_2, \
       ROUND(SUM(Married_couples_3_Households) / SUM(Households) * 100, 2) AS Percentage_Married_Couples_3, \
       ROUND(SUM(Married_couples_3_or_more_Households) / SUM(Households) * 100, 2) AS Percentage_Married_Couples_3_or_more, \
       ROUND(SUM(Married_couples_4_Households) / SUM(Households) * 100, 2)  AS Percentage_Married_Couples_4, \
       ROUND(SUM(Married_couples_5__Households) / SUM(Households) * 100, 2) AS Percentage_Married_Couples_5, \
       ROUND(SUM(Married_couples_None_Households) / SUM(Households) * 100, 2) AS Percentage_Married_Couples_None \
FROM census_test GROUP BY `State/UT`"

result_df = pd.read_sql(query18, engine)

st.write(result_df)

#Households fall below the poverty line in each state based on the power parity categories
st.header("Households fall below the poverty line in each state based on the power parity categories")
query19 = "SELECT `State/UT`, SUM(Power_Parity_Less_than_Rs_45000) AS Households_Below_Poverty_Line \
FROM census_test GROUP BY `State/UT`"

result_df = pd.read_sql(query19, engine)

st.write(result_df)

#The overall literacy rate (percentage of literate population) in each state
st.header("The overall literacy rate (percentage of literate population) in each state")
query20 = "SELECT `State/UT`, \
       ROUND(SUM(Literate) / SUM(Population) * 100, 2) AS Literacy_Rate \
FROM census_test GROUP BY `State/UT`"

result_df = pd.read_sql(query20, engine)

st.write(result_df)