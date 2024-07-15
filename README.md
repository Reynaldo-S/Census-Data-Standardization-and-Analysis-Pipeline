# DE_Tasks
## Census Data Standardization and Analysis Pipeline

The main objective is to transform the raw data into standardized format. This task involves in cleaning,processing and analyzing census data which includes handing missing values, rename the columns etc. The goal is to ensure the census data is uniform,accurate and accessible for futher analysis and visualization.

## Process in Steps:

**1. Loading the Data**

The inital steps invovle in

   * Importing pandas library.
   * Loading the excel file and converting them into a Dataframe.

**2. Rename the Column names**
   
   * To ensure uniformity in the datasets, renaming some of the columns name.

**3. Rename State/UT Names**

   * The State/UT names in the census data are in caps. So formatting the state name with the frist letter of each word in uppercase and rest in lowercase. "and" will use in lowercase.

**4. New State/UT formation**

   * In 2014, Telangana was formed from Andra Pradesh. The State name of the district mentioned in the Telangana.txt are changed to "Tenglana" from "Andra Pradesh".
   * In 2019, Ladakh was formed from Jammu and Kashmir. So renaming the state name to Ladakh for Leh and Kargil districts.

**5. Find and process Missing Data**

   * Filling the missing values with the help of other columns.

**6. Comparing the Missing data before and after filling**

   * Comparing the missing data before and after filling the data

**7. Save Data to MongoDB**

  * Loading the data into MongoDB with the help of pymongo.
  * Ensure you have install Pymongo before proceeding this step.

**8. Database connection and data upload**

  * Fetching the data from MonogDB and loading to into MySQL database.
  * This step invovles in converting the collection into a dataframe, then loading it into MySQL database with the help of Sql Alchemy.
  * Ensure Sql Alchemy have installed.

**9.Run Query on the database and show output on streamlit**

  * Separate python file(census_streamlit.py) is attached to perform the output on streamlit.
  * Ensure Streamlit is installed.
  * Use streamlit run census_streamlit.py command to view the output.
   









