__author__ = 'Saurav Ghosh'
import pandas as pd
import os

def DataQuality(filename,f):  
   df_final = pd.DataFrame()
   dtype =  pd.DataFrame(filename.dtypes)
   dtype.columns = ["DataType"]
   dtype.reset_index(inplace = True)
   dtype.rename(columns = {"index":"ColumnName"},inplace = True)
   dtype["DataType_New"] = [str(dtype.loc[x][1]) for x in range(len(dtype))]
   dtype.drop("DataType",axis =1,inplace = True)
   dtype.rename(columns = {"DataType_New":"DataType"},inplace = True)
 
   numericfeatures    = list(dtype["ColumnName"][~dtype["DataType"].isin(["object","datetime64[ns]"])])
   nonnumericfeatures = list(dtype["ColumnName"][dtype["DataType"] == "object"])

##storing the numeric features in dataframe
   if len(numericfeatures) >0:
       df_numeric = pd.DataFrame(filename[numericfeatures]).describe()
##getting the blank count for numerice features              
       df_numeric_blankvalue = pd.DataFrame(filename[numericfeatures].apply(lambda x: sum(x.isnull()))).T
       df_numeric_blankvalue.index = ["blank_count"]
       df_numeric = df_numeric.append(df_numeric_blankvalue)
##getting the unqiue count for numerice features              
       df_numeric_unqiuevalue   = pd.DataFrame(filename[numericfeatures].apply(lambda x : len(x.unique()))).T
       df_numeric_unqiuevalue.index = ["unique"]
       df_numeric = df_numeric.append(df_numeric_unqiuevalue)            
       df_numeric = df_numeric.T
       df_numeric["totalcount"] = df_numeric["count"] + df_numeric["blank_count"]
       df_numeric["filename"] = f
       df_numeric["datatype"] = "continous_variable"
###min and max length of features
       df_numeric_min = pd.DataFrame([filename[numericfeatures[i]].astype(str).str.len().min() for i in range(len(numericfeatures))])
       df_numeric_min.index = numericfeatures
       df_numeric_min.columns = ["MinLength"]
       df_numeric = pd.merge(df_numeric,df_numeric_min,how="left",left_index = True, right_index = True)
           
       df_numeric_max = pd.DataFrame([ filename[numericfeatures[i]].astype(str).str.len().max() for i in range(len(numericfeatures))])
       df_numeric_max.index = numericfeatures
       df_numeric_max.columns = ["MaxLenght"]       
       df_numeric = pd.merge(df_numeric,df_numeric_max,how="left",left_index = True, right_index = True)
       df_numeric["Possible_Values"] = "Nan"

           
           
########################################################################################################       
##storing the non_numeric features in dataframe       

   if len(nonnumericfeatures) >0:
       df_nonnumeric = pd.DataFrame(filename[nonnumericfeatures]).describe()
##getting the blank count for numerice features              
       df_non_numeric_blankvalue = pd.DataFrame(filename[nonnumericfeatures].apply(lambda x: sum(x.isnull()))).T
       df_non_numeric_blankvalue.index = ["blank_count"]
       df_nonnumeric = df_nonnumeric.append(df_non_numeric_blankvalue)
       df_nonnumeric = df_nonnumeric.T
       df_nonnumeric["totalcount"] = df_nonnumeric["count"] + df_nonnumeric["blank_count"]
       df_nonnumeric["filename"] = f
       df_nonnumeric["datatype"] = "categorical_variable"
       df_nonnumeric.pop("top")
###min and max length of features
       df_nonnumeric_min = pd.DataFrame([filename[nonnumericfeatures[i]].astype(str).str.len().min() for i in range(len(nonnumericfeatures))])
       df_nonnumeric_min.index = nonnumericfeatures
       df_nonnumeric_min.columns = ["MinLength"]
       df_nonnumeric = pd.merge(df_nonnumeric,df_nonnumeric_min,how="left",left_index = True, right_index = True)
           
       df_nonnumeric_max = pd.DataFrame([ filename[nonnumericfeatures[i]].astype(str).str.len().max() for i in range(len(nonnumericfeatures))])
       df_nonnumeric_max.index = nonnumericfeatures
       df_nonnumeric_max.columns = ["MaxLenght"]
       df_nonnumeric = pd.merge(df_nonnumeric,df_nonnumeric_max,how="left",left_index = True, right_index = True)
       possible_values_dict = {}
       for c in filename[nonnumericfeatures]:
           possible_values_dict[c] = filename[c].unique()
        
        
       possible_values =  pd.DataFrame(possible_values_dict.items())
       possible_values.index = possible_values[0]
       possible_values.drop(0,inplace = True, axis = 1)       
       possible_values.rename(columns = {1:"Possible_Values"},inplace = True)
       df_nonnumeric = pd.merge(df_nonnumeric,possible_values, how = "left", left_index = True, right_index = True)
        
   if len(numericfeatures) > 0 :
       df_final = df_final.append(df_numeric)
   if len(nonnumericfeatures) > 0 :
       df_final = df_final.append(df_nonnumeric)

   #drop the column freq
   df_final.drop("freq",axis = 1 , inplace = True)

   return df_final

if __name__ == "__main__":
    dir_path       = r"C:\Users\sauravghosh\Desktop\Machine_Learning\Regression\RidgeRegression\DataSet\test" 
    outputdir_path = r"C:\Users\sauravghosh\Desktop\Machine_Learning\Regression\RidgeRegression\DataSet\output/"
    
    for root,dir_name,file_name in os.walk(dir_path):
        for f in file_name:
            filepath = os.path.join(root,f)
            src_file = pd.read_csv(filepath)
            df_final =  DataQuality(src_file,f)
            output_path = outputdir_path + f
            df_final.to_csv(output_path)
            
        



    
