from operator import index

import pandas as pd
from numpy.ma.core import inner

df = pd.read_csv('https://raw.githubusercontent.com/jorisvandenbossche/pandas-tutorial/master/data/titanic.csv')
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 15)
pd.options.mode.chained_assignment = None

print()

# print(df.loc[[5, 10, 15], ["Name", 'Age']])

# print(df.iloc[[50, 10, 15], [0, 1]])

# print(df.iloc[5:11, :3])

# print(df['Age'][df['Age'] >= 18])

# print(df[df['Age'].isin([5, 9, 15])])

# print(df[(df['Age'] == 5) | (df['Age'] == 9) | (df['Age'] == 15)])

# print(df['Age'])
print(df['Age'].notna())

# print(df.loc[df['Age'].notna(), 'Name'])

# print(df.sort_values('Age').head(10))

# print(df.sort_values(['Age', 'Name'], ascending = [False, True]).head(10)) # сортировкаа по колонке Age в порядке убывания и name в порядке возрастания





#_________________________________________________________________________________________________________

#### РАБОТА С ДВУМЯ DATAFRAME ####

# df2 = df.copy(deep=True) # создание полной копии первого df

# cdf = pd.concat([df, df2]) #объединение двух фреймов: второй следует за пермым по строкам (axis = 0)
# print(cdf.shape, df.shape)
#
# cdf = pd.concat([df, df2],  axis=1) #объединение двух фреймов: второй следует за пермым по столбцам (axis = 0)
# print(cdf.shape, df.shape)


#_________________________________________________________________________________________________________

# mdf = pd.DataFrame(index=df.index)
# mdf['PassengerId'] = df['PassengerId']
# mdf['evenID'] = df['PassengerId'].apply(lambda x: x % 2 == 0)
# pd.merge(df, mdf, how='inner')
# print(pd.merge(df, mdf, how='inner'))

#_________________________________________________________________________________________________________

# print(df.count())

# print(df["Age"].count())

# print(df['Age'].mean(), df['Age'].median())

# print(df['Age'].describe())

#