from sqlalchemy import create_engine
from sqlalchemy.sql.expression import select, text
import sklearn.preprocessing
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import pandas as pd

engine = create_engine('trino://starburst_service@172.31.37.88:8080/system')
connection = engine.connect()

connection.execute(text('create schema if not exists hive.zeekdata'))
df = pd.read_sql_query('''select * from hive.zeekdataacc.conn order by random() limit 3000''', con=connection)

print(df.isnull().sum() / df.shape[0])

df.dropna(inplace=True)
print(len(df))

num_cols = df._get_numeric_data().columns
cat_cols = df.columns[~df.columns.isin(num_cols)]

#print("num cols: {}".format(num_cols))
#print("cat cols: {}".format(cat_cols))
#print("number of services: {}".format(df['service'].unique()))

real_scaler = sklearn.preprocessing.StandardScaler().fit(df[num_cols])
cat_scalers = {}
for col in cat_cols:
    srs = df[col].apply(str)
    cat_scalers[col] = sklearn.preprocessing.LabelEncoder().fit(srs.values)

df_trans = df.copy()
df_trans[num_cols] = real_scaler.transform(df[num_cols].values)
for col in cat_cols:
    string_df = df[col].apply(str)
    df_trans[col] = cat_scalers[col].transform(string_df)
    

X = df_trans[df.columns[~df.columns.isin(["service"])]]
y = df_trans["service"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

model = LogisticRegression(random_state=1234)
model.fit(X_train, y_train)
y_hat = model.predict(X_test)
prediction = cat_scalers["service"].inverse_transform(y_hat)
true_y = cat_scalers["service"].inverse_transform(y_test)
print(classification_report(true_y, prediction))
