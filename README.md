# PIZZAS 2016

### INTRODUCTION

This code is an ETL which predicts the ingredients that a certain pizzeria
has to buy in a specific week based on various DataFrames from the orders
made in the year 2016. We have to manually clean the datasets using re and pd.

Then, we load the results in an xml file. We also load the data description to another xml file

Disclaimer: some variables and comments are in spanish as it is my native tongue

### 1. Libraries used
- 'pandas': used for managing the DataFrame
- 're': used for splitting strings and detecting re patterns
- 'warnings': to ignore certain warnings concerning data types
- 'xml': for writing the xml file
- 'datetime': for the datetime format on some columns

### 2. ETL
#### 1) Extract
We simply read the csvs with the names on the list 'file_names' with the 'read_csv' function from pandas, controlling whether the separation is ',' or ';'

#### 2) Transform
It gets the prediction of the ingredients for the week 'semana'. My prediction is calculated using the previous and following week, aswell as the one asked. 

I use certain functions such as 'compilar_patrones', which compiles the re patterns, or 'drop_nans', which drops the nans from both dataframe keeping track of the data it has dropped. The various orders are in different dataframes. if the information about an order in df_orders is not valid, we must discard this order in df_order_details and so on.

We use the function 'limpieza_de_datos' which cleans the dataframe. Once the dataframes are clean, we proceed to the prediction

We calculate the pizzas ordered for each week with both 'orders' and 'order_details' dataframes and save it in a dictionary 'pizzas_dict'. If the week we are calculating is the previous or following, we multiply it by 0.3, whereas the current one is multiplied by 0.4. That way, we give more importance to the actual week but also even it out with the other ones. 

Once we have the dictionary, we use the dataframe 'pizza_types_df', which contains the pizzas and their ingredients. Depending on the size of the pizza, we multiply it by 0.75 (m), 1 (m), 1.5 (l), 2 (xl), and 3 (xxl). Finally, we gather the data from the ingredients prediction in a dictionary 'ingredients_dict'

Once we have obtained the 'ingredients_dict', we have to load the information in an xml file. We create the root and get the ingredients and their quantities in two lists. Then we iterate through these lists adding the information to the tree. Finally, we write the information in an xml file

Lastly, we want to create an informative xml on the data types of each DataFrame's columns, aswell as the nans and nulls on each one of them. We use the same functions from the standard python library 'xml' as in the previous function. 

#### 3) Load
Once we have obtained the 'ingredients_dict', we have to load the information in an xml file. We create the root and get the ingredients and their quantities in two lists. Then we iterate through these lists adding the information to the tree. Finally, we write the information in an xml file

Lastly, we want to create an informative xml on the data types of each DataFrame's columns, aswell as the nans and nulls on each one of them. We use the same functions from the standard python library 'xml' as in the previous function. 
