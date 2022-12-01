import pandas as pd
import datetime
import re
import warnings
import xml.etree.ElementTree as et


warnings.filterwarnings("ignore")

def extract()->list[pd.DataFrame]:
    file_names = ['data_dictionary.csv', 'order_details.csv', 'orders.csv', 'pizza_types.csv','pizzas.csv']
    df_lst = []
    for name in file_names:
        if name in ['data_dictionary.csv','pizzas.csv','pizza_types.csv']:
            sep = ','
        else:
            sep = ';'
        df = pd.read_csv(f'files2016/{name}', sep, encoding='latin_1')
        df_lst.append(df)
    return df_lst

def compilar_patrones():
    espacio = re.compile(r'\s')
    guion = re.compile(r'-')
    arroba = re.compile(r'@')
    d_0 = re.compile(r'0')
    d_3 = re.compile(r'3')
    uno = re.compile(r'one',re.I)
    dos = re.compile(r'two',re.I)
    comma = re.compile(r',')
    espacio = re.compile(r'\s')
    
    quitar = [espacio, guion, arroba, d_0, d_3, uno, dos]
    poner = ['_', '_', 'a', 'o', 'e', '1', '2']
    patrones = [quitar, poner, comma]
    return patrones

def drop_nans(df_orders:pd.DataFrame, df_order_details:pd.DataFrame):
    """
    Dropeamos los NaNs de ambos dataframes. Intersecamos ambos dataframes
    para droppear lo que hemos sacado de un dataframe en el otro
    """
    df_order_details.dropna(inplace=True)
    or_id_A = set(df_orders['order_id'].unique())
    or_id_B = set(df_order_details['order_id'].unique())
    keep_order_id = or_id_A & or_id_B
    df_orders = df_orders[df_orders['order_id'].isin(keep_order_id)]
    df_order_details = df_order_details[df_order_details['order_id'].isin(keep_order_id)]
    # Ordenamos los Dataframes y reiniciamos sus índices
    df_orders.sort_values(by='order_id', inplace=True)
    df_orders.reset_index(drop=True, inplace=True)
    df_order_details.sort_values(by='order_id', inplace=True)
    df_order_details.reset_index(drop=True, inplace=True)
    return df_orders, df_order_details

def transform_key(key):
    if key[-1] == 's':
        end_str, count = 2, 0.75
    elif key[-1] == 'm':
        end_str, count = 2, 1
    elif key[-1] == 'l' and key[-2] != 'x':
        end_str, count = 2, 1.5
    elif key[-2:] == 'xl' and key[-3] != 'x': # xl
        end_str, count = 3, 2
    else: # xxl
        end_str, count = 4, 3
    return end_str, count

def limpieza_de_datos(df_orders:pd.DataFrame, df_order_details:pd.DataFrame):
    ### LIMPIEZA DE LOS DATOS
    ## 1. FORMATO DATETIME
    for i in range(len(df_orders)):
        unformatted_date = str(df_orders['date'][i])
        df_orders.loc[i,'date'] = pd.to_datetime(df_orders['date'][i], errors='coerce')
        if pd.isnull(df_orders.loc[i,'date']):
            unformatted_date = unformatted_date[:unformatted_date.find('.')]
            formatted_date = datetime.datetime.fromtimestamp(int(unformatted_date))
            df_orders.loc[i,'date'] = pd.to_datetime(formatted_date)

    df_orders['date'] = pd.to_datetime(df_orders['date'], format="%Y/%m/%d")
    df_orders['week'] = df_orders['date'].dt.week
    df_orders['weekday'] = df_orders['date'].dt.weekday

    ## 2. CORREGIR NOMBRES

    df_orders, df_order_details = drop_nans(df_orders, df_order_details)
    patrones = compilar_patrones()
    [quitar, poner, comma] = patrones
    # Ahora debo corregir los nombres de las pizzas y los números
    for i in range(len(quitar[:-2])):
        df_order_details['pizza_id'] = [quitar[i].sub(poner[i], str(x)) for x in df_order_details['pizza_id']]
    for i in range(len(quitar[:-2]), len(quitar)):
        df_order_details['quantity'] = [quitar[i].sub(poner[i], str(x)) for x in df_order_details['quantity']]
    df_order_details['quantity'] = [abs(int(x)) for x in df_order_details['quantity']]

    
    return df_orders, df_order_details, comma

def transform(df_lst:list[pd.DataFrame], semana):
    df_orders = df_lst[2]
    df_order_details = df_lst[1]
    pizza_types_df = df_lst[3]
    pizzas_df = df_lst[4]
    df_orders.dropna(subset='date', inplace=True)
    df_orders.reset_index(drop=True, inplace=True)
    df_orders.drop('time', axis=1, inplace=True)

    df_orders, df_order_details, comma = limpieza_de_datos(df_orders, df_order_details)

    ### OBTENER PREDICCIÓN
    ## 1. OBTENER DF DE SEMANA ANTERIOR, ACTUAL Y POSTERIOR Y SUMARLAS PONDERADAMENTE
    pizzas_df = df_lst[4]
    pizzas_dict = {}
    for index in range(len(pizzas_df)):
        pizzas_dict[pizzas_df['pizza_id'][index]] = 0
    
    for week in range(-1,2):
        
        week_orders = df_orders.loc[df_orders['week']==semana+week]
        order_ids = week_orders['order_id']
        week_df = df_order_details.loc[df_order_details['order_id'].isin(order_ids)]
        week_df.reset_index(drop=True)
        
        if week in [-1,1]:
            for index in range(1,len(week_df)):
                # Así accedemos a un valor concreto: df.iloc[columna].iloc[fila]
                pizzas_dict[week_df['pizza_id'].iloc[index]] += 0.3*week_df['quantity'].iloc[index]
        else:
            for index in range(1,len(week_df)):
                # Así accedemos a un valor concreto: df.iloc[columna].iloc[fila]
                pizzas_dict[week_df['pizza_id'].iloc[index]] += 0.4*week_df['quantity'].iloc[index]

    espacio = re.compile(r'\s')
    
    # Obtenemos los ingredientes
    for key in pizzas_dict.keys():
        pizzas_dict[key] = round(pizzas_dict[key])

        ingredients_dict = {}
        for pizza1_ingredients in pizza_types_df['ingredients']:
            pizza1_ingredients = espacio.sub('',pizza1_ingredients)
            ingredients = comma.split(pizza1_ingredients)
            for ingredient in ingredients:
                if ingredient not in ingredients_dict:
                    ingredients_dict[ingredient] = 0
        
        for key in pizzas_dict:
            end_str, count = transform_key(key)
            
            pizza = key[:-end_str]
            current_pizza_ingredients = pizza_types_df[pizza_types_df['pizza_type_id'] == pizza]["ingredients"].head(1).item()
            current_pizza_ingredients = espacio.sub('',current_pizza_ingredients)
            ingredients_lst = comma.split(current_pizza_ingredients)
            
            for ingredient in ingredients_lst:
                ingredients_dict[ingredient] += count

        # Multiplicamos el diccionario por 1.2 para tener un margen de ingredientes
        # y redondeamos el resultado (ya que no podemos tener fracciones de ingredientes)
        for key in ingredients_dict.keys():
            ingredients_dict[key] = round(ingredients_dict[key]*1.2)

    return df_orders, df_order_details, ingredients_dict

def load_ingredients(ingredients_dict: dict, semana):
    root = et.Element("INGREDIENTES", name=f"Ingredientes de la semana {semana}")
    ingredients_lst = list(ingredients_dict.keys())
    quantity_lst = list(ingredients_dict.values())
    quantity_lst = [str(x) for x in quantity_lst]

    for iter in range(len(ingredients_dict)):
        name = et.SubElement(root, 'Ingredient', name = ingredients_lst[iter])
        quantity = et.SubElement(name, 'Quantity', quantity = quantity_lst[iter])

    arbol = et.ElementTree(root)
    arbol.write(f'ingredients_week_{semana}.xml')

def load_dtypes(dict_dtypes):
    root = et.Element("DTYPES", name='Dtypes of Maven Pizzas DataFrames')
    
    for df in dict_dtypes:
        name = et.SubElement(root, 'Dataframe', name = str(df))
        for column in dict_dtypes[df]:
            column_name = et.SubElement(name, 'Column', column_name = str(column))
            for characteristic in dict_dtypes[df][column]:
                data = et.SubElement(column_name, 'Data', data = str(characteristic), info= str(dict_dtypes[df][column][characteristic]))
                
    
    arbol = et.ElementTree(root)
    arbol.write('original_df_info.xml')

def obtain_types(df_lst: list[pd.DataFrame]):
    file_names = ['data_dictionary', 'order_details', 'orders', 'pizza_types','pizzas']
    column_types = {}
    for i in range(len(df_lst)):
        df = df_lst[i]
        column_types[file_names[i]] = {}
        for column in df:           
            column_types[file_names[i]][str(column)] = {
                                                   'dtype': df[column].dtype,
                                                   'nans': df[column].isna().sum(),
                                                   'nulls': df[column].isnull().sum()
                                                   }
    return column_types

if __name__ == "__main__":
    df_lst = extract()
    semana = 25
    df_orders, df_order_details, ingredients_dict = transform(df_lst, semana)
    column_types = obtain_types(extract())
    load_ingredients(ingredients_dict, semana)
    load_dtypes(column_types)
