import pandas as pd
import stage_one

# publish_sheet()
# upload_mongo()
# post_service()
filepath = r'D:\Users\nou\PycharmProjects\fullmission\data.xlsx'
xl = pd.ExcelFile(filepath)
machines = xl.sheet_names
for each_machine in machines:
    df = pd.read_excel(filepath, each_machine)
    stage_one.read_sheet(df, each_machine)
    stage_one.cal_agg(df, each_machine)
