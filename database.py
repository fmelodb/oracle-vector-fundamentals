import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

import oracledb
import os
import array
from dotenv import load_dotenv

columns_list    = "" # list of customer columns
customer_name   = "" # get similar customers to this name
distance_metric = "" # distance metric

# get database connection
def get_connection():
    load_dotenv()
    dsn = os.getenv("DB_URL")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")    

    try:
        connection = oracledb.connect(user=username, password=password, dsn=dsn)        
        return connection
    except Exception as e:
        print("Connection failed!")
 
# create database schema        
def create_schema():
    with connection.cursor() as cursor:
        
        drop_table = "drop table if exists customer"
        
        create_table = """create table customer (
                            id            int generated as identity primary key,
                            name          varchar2(30) not null,
                            age           int not null,
                            income        int not null)"""
        
        sql = [drop_table, create_table]
        
        for s in sql:
            try:
                cursor.execute(s)                
            except oracledb.DatabaseError as e:
                raise                
        insert_data()

# insert sample data            
def insert_data():    
    data_to_insert = [
        ("John",    28, 6500),
        ("Jessica", 22, 7000),
        ("David",   31, 3900),        
        ("Matthew", 38, 5200),
        ("Brandon", 35, 4700),
        ("Joshua",  25, 2600),
        ("Amanda",  33, 6500),        
        ("Lauren",  39, 5500),
        ("James",   26, 3200),
        ("Olivia",  44, 8200) 
    ]
    
    cursor = connection.cursor()
    sql = "INSERT INTO customer (name, age, income) VALUES (:1, :2, :3)"
    cursor.executemany(sql, data_to_insert)
    connection.commit()    
   
# vectorize data    
def vectorize_data(column_list):
    # add/remove vector column (modify vector column not supported)
    remove_vector_column = "alter table customer drop column profile"
    add_vector_column = "alter table customer add profile vector(" + str(len(column_list.split(","))) + ", float32)"    
    cursor = connection.cursor()
    try:   
        cursor.execute(remove_vector_column)  # remove if exists
        cursor.execute(add_vector_column)     # add if not exists           
    except oracledb.DatabaseError as e:
        cursor.execute(add_vector_column)     # remove throws error if not exists, then add
    
    # vectorize rows
    with connection.cursor() as cursor:
        sql_query = "select id, " + column_list + " from customer"
        cursor.execute(sql_query)       
        query_resultset = cursor.fetchall()
        
        binds = []
        for row in query_resultset:
            id = row[0]
            columns = row[1:]
            profile_embedding = array.array('f', columns)     
            binds.append((profile_embedding, id))                 
        
        sql_update = "update customer set profile = :1 where id = :2"
        cursor.executemany(sql_update, binds)
        connection.commit()        

# get list of customers        
def get_customers():
    with connection.cursor() as cursor:
        sql = "select id, name, age, income from customer"
        cursor.execute(sql)   
        rs = cursor.fetchall()
        
        colunas = [col[0] for col in cursor.description]
        df = pd.DataFrame(rs, columns=colunas)
        print(df.to_string(index=False))        

# get customer vectorized profiles 
def get_customer_profiles():
    with connection.cursor() as cursor:
        sql = "select name, profile from customer"
        cursor.execute(sql)   
        query_resultset = cursor.fetchall()
        return query_resultset
   
# perform similarity search (return topK=3)    
def get_similiar_customer_profiles(column_list, name, k=3, distance="euclidean"):
    with connection.cursor() as cursor:
        sql = "select profile from customer where name = :1"
        cursor.execute(sql, [name])   
        embedding_profile = cursor.fetchone()
        
        sql = "select name, profile from customer order by vector_distance(profile, :1, distance_metric) fetch first :2 rows only"
        sql = sql.replace("distance_metric", distance)
                
        cursor.execute(sql, [embedding_profile[0], k])   
        similarity_resultset = cursor.fetchall()
        return similarity_resultset

# set list of columns to compare 
def set_column_list(columns):
    global columns_list
    columns_list = columns.lower()
    
    col_list = columns_list.split(",")      
    
    if not len(col_list) == 2:
        raise Exception("Specify 1 or 2 columns") 
          
    for item in col_list:
        if item not in ["age", "income", "is_single", "no_children", "no_cars"]:
            raise Exception("Column list should contain only age, inome, is_single, no_children or no_cars")
    
# set customer name to get similarity        
def set_customer(name):
    global customer_name
    customer_name = name.title()
    
    if customer_name.title() not in ["Jessica", "David", "Matthew", "Brandon","Joshua", "Amanda", "Lauren", "James", "Olivia"]:
        raise Exception("Customer name must be one of Jessica, David, Matthew, Brandon, Joshua, Amanda, Lauren, James, Olivia")

# set similarity distance metric    
def set_distance_metric(distance):
    global distance_metric
    distance_metric = distance.upper()
    if distance_metric.upper() not in ["COSINE", "EUCLIDEAN", "EUCLIDEAN_SQUARED", "HAMMER", "MANHATTAN", "JACCARD", "DOT"]:
        raise Exception("Distance must be one of CONSINE, EUCLIDEAN, EUCLIDEAN_SQUARE, MANHATTAN, JACCARD, DOT")
    
# get full chart
def customers_chart(columns, distance="euclidean"):
    set_column_list(columns)
    set_distance_metric(distance)
    
    create_schema()    
    vectorize_data(columns_list)
    
    all_profiles = get_customer_profiles()    
    
    cols = columns_list.split(",")
    data = {
        'Name':  [item[0] + '[' + str(int(item[1][0])) + ',' + str(int(item[1][1])) + ']'   for item in all_profiles],
        cols[0]: [item[1][0] for item in all_profiles],
        cols[1]: [item[1][1] for item in all_profiles]
    }
    
    # Config chart
    df = pd.DataFrame(data)
    ax = sns.lineplot(data=df, x=cols[0], y=cols[1], hue='Name', marker='o')
        
    for i, row in df.iterrows():
        ax.text(
            row[cols[0]], 
            row[cols[1]],
            str(row['Name']),
            color='black',
            ha='left', 
            va='top', 
            fontsize=7,
            fontweight='normal'
        )
    
    # remove legend
    plt.legend().remove()        
    return plt

# get similarity chart    
def similarity_chart(columns, cust_name, distance="euclidean"):
    set_column_list(columns)
    set_distance_metric(distance)
    set_customer(cust_name)
    
    connection = get_connection()
    create_schema()    
    vectorize_data(columns_list)
    
    similiar_profiles = {s[0] for s in get_similiar_customer_profiles(columns_list, customer_name, 3, distance_metric)}    
    all_profiles = get_customer_profiles()    
    
    cols = columns_list.split(",")
    data = {
        'Name':  [item[0]    for item in all_profiles],
        cols[0]: [item[1][0] for item in all_profiles],
        cols[1]: [item[1][1] for item in all_profiles]
    }

    # Config chart
    df = pd.DataFrame(data)
    ax = sns.lineplot(data=df, x=cols[0], y=cols[1], hue='Name', marker='o')
        
    for i, row in df.iterrows():
        ax.text(
            row[cols[0]], 
            row[cols[1]],
            str(row['Name']),
            color='red'    if str(row['Name']) == customer_name else 'black',
            ha='left', 
            va='top', 
            fontsize=10       if str(row['Name']) in similiar_profiles else 7,
            fontweight='bold' if str(row['Name']) in similiar_profiles else 'normal'
        )
    
    
    # Remove a legenda
    plt.legend().remove()    
    return plt

def init():
    global connection
    connection = get_connection()
    

    
   