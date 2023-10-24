# -*- coding: utf-8 -*-
"""
Created on Wed Oct  4 16:19:41 2023

@author: ASUS VN
"""



import pandas as pd
from unidecode import unidecode

# Định nghĩa hàm loại bỏ dấu từng giá trị trong DataFrame
def remove_accents_from_dataframe(dataframe):
    for column in dataframe.columns:
        dataframe[column] = dataframe[column].apply(lambda x: unidecode(str(x)) if pd.notna(x) else x)
    return dataframe

# Đường dẫn đến tệp Excel đầu vào và đầu ra
excel_input_path = "D:\Xu_ly_du_lieu_lon\data_bds_clean_2023_new_19_New.xlsx"
excel_output_path = "D:\Xu_ly_du_lieu_lon\data_bds_clean_2023_new_19_Khongdau.xlsx"

# Đọc tệp Excel vào DataFrame
df = pd.read_excel(excel_input_path, engine='openpyxl')

# Loại bỏ dấu từng giá trị trong DataFrame
df_cleaned = remove_accents_from_dataframe(df)

# Ghi DataFrame đã được loại bỏ dấu vào tệp Excel mới
df_cleaned.to_excel(excel_output_path, index=False, engine='openpyxl')

print("Đã hoàn thành quá trình loại bỏ dấu và lưu vào file mới.")

data=pd.read_excel(r"D:\Xu_ly_du_lieu_lon\data_bds_clean_2023_new_19_Khongdau.xlsx")
data.to_csv(r"D:\Xu_ly_du_lieu_lon\data_bds_clean_2023_new_19_Khongdau_new.csv",index=False, encoding='utf8')

#%%



import pandas as pd
from unidecode import unidecode

# Định nghĩa hàm loại bỏ dấu từng giá trị trong DataFrame
def remove_accents_from_dataframe(dataframe):
    for column in dataframe.columns:
        dataframe[column] = dataframe[column].apply(lambda x: unidecode(str(x)) if pd.notna(x) else x)
    return dataframe

# Đường dẫn đến tệp Excel đầu vào và đầu ra
excel_input_path = "D:\Xu_ly_du_lieu_lon\ds_atm_Khongdau.xlsx"
excel_output_path = "D:\Xu_ly_du_lieu_lon\ds_atm_Khongdau_ACB.xlsx"


# Đọc tệp Excel vào DataFrame
df = pd.read_excel(excel_input_path,sheet_name='ACB',header = 0, engine='openpyxl')

# Loại bỏ dấu từng giá trị trong DataFrame
df_cleaned = remove_accents_from_dataframe(df)

# Ghi DataFrame đã được loại bỏ dấu vào tệp Excel mới
df_cleaned.to_excel(excel_output_path, index=False, engine='openpyxl')
