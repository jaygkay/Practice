import sqlalchemy
import pandas as pd
def redshift_data(your_primary_key):
	'''
	This function takes 'imei number' and 'trip_start'
	This function returns a dataframe from redshift that contains all feature with trip_start as a key

	ex) redshift_data(str_imei, str_trip_start)
	'''
	engine = sqlalchemy.create_engine('your_redshift_endpoint')
	trip_query_df = pd.read_sql_query("your_sql_query;", engine)
	trip_query_df = trip_query_df.sort_values(['your_primary_key'])
	trip_query_df = trip_query_df.reset_index()

	return trip_query_df
