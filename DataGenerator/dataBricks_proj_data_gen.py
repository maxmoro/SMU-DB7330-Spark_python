# Databricks notebook source
# For Data Generation
import numpy as np
import pandas as pd
import random
import matplotlib.pyplot as plt
import seaborn as sns
import string

# COMMAND ----------

class DataGenerator:
    def __init__(self, rows = 20, cols_float = 2, cols_int = 2, cols_words=3, cols_group=3
                 ,numrange = 12, strrange = 500, draw = False, wordsCount=5, groupsElements=10, groupsWordsCount=2):
        self.rows = rows
        self.cols_float = cols_float
        self.cols_int = cols_int
        #self.cols_string = cols_string
        self.cols_words = cols_words
        self.numrange = numrange 
        self.strrange = strrange
        self.wordsCount = wordsCount
        self.cols_group = cols_group
        self.groupsWordsCount=groupsWordsCount
        self.groupsElements = groupsElements
        self.draw = draw
        #https://github.com/dwyl/english-words
        words_file="https://computersciencewiki.org/images/1/13/Words_alpha.txt"
        self.words = pd.read_csv(words_file, sep=" ", header=None)
    
    ####
    # Add back to main datagenerator
    ####
    def set_rows(self, nrows):
      self.rows = nrows
      
    def print(self):
      print ("Rows: {}".format(self.rows))
      print ("Float Columns: {}".format(self.cols_float))
      print ("Integer Columns: {}".format(self.cols_int))
      # print ("String Columns: {}".format(self.cols_string))
      # Add more as needed
          
    #### 
    
          
    def generateData(self):
        df = pd.DataFrame()
        random.seed(1)
        
        for i in range(self.cols_float):
            col_name =  'float' + str(i)
            
            seed = random.randint(0,10000) 
            df[col_name] = self.lognuniform(use_int = False, seed = seed)
            if self.draw == True:
                self.plot_density(df[col_name])
                     
        for i in range(self.cols_int):
            col_name = 'int' + str(i)
            seed = random.randint(0,10000) 
            df[col_name] = self.lognuniform(use_int = True, seed = seed)
            if self.draw == True:
                self.plot_density(df[col_name])
        
        for i in range(self.cols_words):
            col_name = 'words' + str(i)
            df[col_name] = self.randomWords() #(N = n)
        
        for i in range(self.cols_group):
            col_name = 'group' + str(i)
            df[col_name] = self.randomGroups() #(N = n)
        """
        for i in range(self.cols_string):
            col_name = 'string' + str(i)
            random.seed(random.randint(0,self.strrange))
            #n = random.randint(0,self.strrange) 
            df[col_name] = self.randomString() #(N = n)
        """
        return df         
    
    def lognuniform(self, base=np.e, use_int = True, seed = 0):
        np.random.seed(seed) 
        # Note: you can not use random.seed here since the np.random.uniform needs np.random.seed not random.seed.
        multiplier = np.random.choice([-1,1], size= self.rows)
        # -5 is used since we done want most of the value to be less than base
        exponentials = np.random.uniform(low = -5, high = self.numrange, size = self.rows)
        #data = np.power(base, np.random.uniform(low = -self.numrange, high = self.numrange, size = self.rows))
        data = np.power(base, exponentials)  * multiplier
        if (use_int == True):
            return data.astype(int)
        if (use_int == False):
            return data
    
    """
    def randomString(self):
        mylist = []
        # Options are Uppercase letters, lowercase letters, spaces (*10)
        options = (string.ascii_letters + string.digits + string.punctuation + ' '*10)
        for i in range(self.rows):
            mystring = ''.join(random.choice(options) for i in range(random.randint(0,self.strrange)))
            mylist.append(mystring)
        return mylist
    """

    def randomWords(self):
        mylist = []
        for i in range(self.rows):
            mystring = (' '.join(str(self.words.iat[random.randint(0,self.words.shape[0]-1),0]) for i in range(random.randint(1,self.wordsCount)))).strip()
            mylist.append(mystring)
        return mylist
    
    def randomGroups(self):
        groups = []
        for i in range(self.groupsElements):
            mystring = (' '.join(str(self.words.iat[random.randint(0,self.words.shape[0]-1),0]) for i in range(random.randint(1,self.groupsWordsCount)))).strip()
            groups.append(mystring)
        mylist = []
        for i in range(self.rows):
            mystring = groups[random.randint(0,len(groups)-1)]
            mylist.append(mystring)
        return mylist
    
    def plot_density(self, col):
        count, bins, ignored = plt.hist(col, 15, density=True)
        plt.plot(bins, np.ones_like(bins), linewidth=2, color='r')
        plt.show()                

# COMMAND ----------

datagen = DataGenerator(rows = 10000, cols_float = 20, cols_int = 20, cols_words=10, cols_group=11, wordsCount = 10)

# COMMAND ----------

df = datagen.generateData()

# COMMAND ----------

file_loc = "/dbfs"
file_prefix = "/mnt/blob/"
file_name = "test_dataset1"
dbutils.fs.mkdirs(file_prefix)

# COMMAND ----------

# Remove existing files for new run
dbutils.fs.rm(file_prefix + file_name + ".csv")
# Write new file (needs /dbfs added to the begining of file name)
df.to_csv(file_loc + file_prefix + file_name + ".csv", index = False)
#df.to_csv('/FileStore/'+file_name + ".csv", index = False)

# COMMAND ----------

# Writing 100 new rows
datagen.set_rows(100) 
datagen.print()
data2 = datagen.generateData()
data2.shape

# Remove existing files for new run
dbutils.fs.rm(file_prefix + file_name + "_100.csv")
# Write new file (needs /dbfs added to the begining of file name)
data2.to_csv(file_loc + file_prefix + file_name + "_100.csv", index = False)

# COMMAND ----------

# Writing 1000 new rows
datagen.set_rows(1000) 
datagen.print()
data2 = datagen.generateData()
data2.shape

# Remove existing files for new run
dbutils.fs.rm(file_prefix + file_name + "_1000.csv")
# Write new file (needs /dbfs added to the begining of file name)
data2.to_csv(file_loc + file_prefix + file_name + "_1000.csv", index = False)

# COMMAND ----------

# Writing 10000 new rows
datagen.set_rows(10000) 
datagen.print()
data2 = datagen.generateData()
data2.shape

# Remove existing files for new run
dbutils.fs.rm(file_prefix + file_name + "_10000.csv")
# Write new file (needs /dbfs added to the begining of file name)
data2.to_csv(file_loc + file_prefix + file_name + "_10000.csv", index = False)
