import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'a-krivosheina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 1),
    }


@dag(default_args=default_args, schedule_interval='0 10 * * *', catchup=False)

def sales_all_a_krivos():
       
    @task()
    def get_df_vgsales_2010():
        df_vgsales_all = pd.read_csv(path)
        df_vgsales_2010 = df_vgsales_all.query('Year==2010.0')
        return df_vgsales_2010
    
       
    @task()
    def get_most_sales_games(df_vgsales_2010):
        most_sales_games = df_vgsales_2010.groupby('Name', as_index = False) \
                                  .agg({'Global_Sales':'sum'}) \
                                  .sort_values('Global_Sales', ascending = False) \
                                  .head(1)
        
        most_sales_games = most_sales_games.Name.tolist()   
        return most_sales_games

    @task()
    def get_EU_most_popular_genre(df_vgsales_2010):
        EU_most_popular_genre =  df_vgsales_2010.groupby('Name', as_index = False) \
                                  .agg({'EU_Sales':'sum'}) \
                                   .sort_values('EU_Sales', ascending = False) 
        EU_most_popular_genre = EU_most_popular_genre[EU_most_popular_genre['EU_Sales'] == EU_most_popular_genre['EU_Sales']. max ()]
        EU_most_popular_genre = EU_most_popular_genre.Name.tolist()
        return EU_most_popular_genre

    
    
    @task()
    def get_NA_million_platform(df_vgsales_2010):
        NA_million_platform = df_vgsales_2010.query('NA_Sales > = 1.00').groupby('Platform', as_index = False) \
                                      .agg({'EU_Sales':'mean'}) \
                                       .sort_values('EU_Sales', ascending = False)
        
        NA_million_platform = NA_million_platform.Platform.tolist()
        return NA_million_platform

    
    
    @task()
    def get_avg_japan_mean(df_vgsales_2010):
        avg_japan_mean =  df_vgsales_2010.groupby('Publisher', as_index = False) \
                                 .agg({'JP_Sales':'mean'}) \
                                 .sort_values('JP_Sales', ascending = False)
                           
        avg_japan_mean = avg_japan_mean[avg_japan_mean['JP_Sales'] == avg_japan_mean['JP_Sales']. max ()]
        avg_japan_mean = avg_japan_mean.Publisher.tolist()
        return avg_japan_mean
    
    
    @task()
    def get_sales_EU_vs_JP(df_vgsales_2010):
        sales_EU_vs_JP = df_vgsales_2010[df_vgsales_2010['EU_Sales'] > df_vgsales_2010['JP_Sales']].Rank.count()
        return sales_EU_vs_JP
    
    
    @task()
    def print_data(most_sales_games, EU_most_popular_genr, NA_million_platform, avg_japan_mean, sales_EU_vs_JP):

        context = get_current_context()
        date = context['ds']    

        print(f'Most_sales_games for date {date}')
        print(*most_sales_games, sep = '\n')

        print(f'EU_most_popular_genr for date {date}')
        print(*EU_most_popular_genr, sep = '\n')

        print(f'NA_million_platform for date {date}')
        print(*NA_million_platform, sep = '\n')
        
        print(f'Avg_japan_mean for date {date}')
        print(*avg_japan_mean, sep = '\n')
        
        print(f'Sales_EU_vs_JP for date {date}')
        print(sales_EU_vs_JP)
        
 
    df_vgsales_2010 = get_df_vgsales_2010()
    most_sales_games_data = get_most_sales_games(df_vgsales_2010)
    EU_most_popular_genr_data = get_EU_most_popular_genre(df_vgsales_2010)
    NA_million_platform_data = get_NA_million_platform(df_vgsales_2010)
    avg_japan_mean_data = get_avg_japan_mean(df_vgsales_2010)
    sales_EU_vs_JP_data = get_sales_EU_vs_JP(df_vgsales_2010)

    print_data(most_sales_games_data, EU_most_popular_genr_data, NA_million_platform_data, avg_japan_mean_data, sales_EU_vs_JP_data)

sales_all_a_krivos = sales_all_a_krivos()

   
