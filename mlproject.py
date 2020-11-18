import pandas as pd
import numpy as np
from pandas.api.types import is_numeric_dtype
import os.path


## File modification
# This aims to standardize the two files so they can be processed in the same way.

def hf_add_definitions(input_file_name, output_file_name, definition) :
    input_file = open(input_file_name,"r")
    text = definition + "\n" + input_file.read()
    input_file.close()
    
    output_file = open(output_file_name, "w")
    output_file.write(text)
    output_file.close()
    
def hf_remove_characters(text, characters) :
    for character in characters : 
        text = text.replace(character,'')    
    return text

def hf_remove_id(input_file_name) :
    input_file = open(input_file_name,"r")
    text = ""    
    for line in input_file : 
        text += line[line.index(',') + 1:]        
    input_file.close()    
    return text
    

def prepare_kidney_file(input_file_name, output_file_name) :
    text = hf_remove_id(input_file_name) # This removes the id column
    text = hf_remove_characters(text, ["\t", "?", ' ']) # This removes tabs, question marks, and spaces from the file
    
    output_file = open(output_file_name, "w")
    output_file.write(text)
    output_file.close()
    
def prepare_bank_file(input_file_name, output_file_name) :
    hf_add_definitions(input_file_name, output_file_name, "variance,skewness,curtosis,entropy,class") # This adds a definition to the file
    

unprepared_banknote_file_name = "data_banknote_authentication.txt"
banknote_file_name = "data_banknote_authentication_with_def.csv"
if not(os.path.exists(banknote_file_name)) :
    prepare_bank_file(unprepared_banknote_file_name, banknote_file_name)

unprepared_kidney_file_name = "archive/kidney_disease.csv" # This seems to work even on Windows, but this is a point where we should be careful 
kidney_file_name = "archive/kidney_disease_cleaned.csv"
if not(os.path.exists(kidney_file_name)) :
    prepare_kidney_file(unprepared_kidney_file_name, kidney_file_name)
    

## Import the files into pandas dataframes

def import_file(file_name, separator) :    
    pd_data = pd.read_csv(file_name, sep = separator)
    return pd_data
    
banknote_pd_data = import_file(banknote_file_name, ",")    
kidney_pd_data = import_file(kidney_file_name, ",")


## Clean dataframes
# Add missing values
# Center and reduce columns

def hf_get_mean_value(column) :
    # Help function that returns the mean value of the column.
    # For non numeric data types, returns the most frequent value.
    if is_numeric_dtype(column) :
        return column.mean()
    else :
        values = pd.value_counts(column)
        return values.idxmax()
        

def clean_dataframe(pd_data) :
    column_names = pd_data.columns
    number_of_columns = column_names.size
    means = []
    
    for col_name in column_names :
        means.append(hf_get_mean_value(pd_data[col_name]))
        
    # We have the mean value of each column of the dataset
    # Now we find the cells that are not filled, and replace them with the mean value of the column.
    
    null_data = np.where(pd.isnull(pd_data))
    for i in range(len(null_data[0])) :
        row = null_data[0][i]
        col_id = null_data[1][i]
        col = column_names[col_id]
        pd_data.at[row,col] = means[col_id]
    
    # Our cells are all filled now.
    # We can center and reduce the values of the numeric columns
    
    pd_data = pd_data.apply(lambda x : (x - x.mean()) / np.sqrt(x.var() + 10**-9) if is_numeric_dtype(x) else x) # The value 10**-9 is a safety to ensure we don't divide by 0.
    
    return pd_data

kidney_pd_data = clean_dataframe(kidney_pd_data)
banknote_pd_data = clean_dataframe(banknote_pd_data)