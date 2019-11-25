#import the pandas library and aliasing as pd
import pandas as pd
df = pd.DataFrame()
print(df)

import pandas as pd
data = [1,2,3,4,5]
df = pd.DataFrame(data)
print(df)


import pandas as pd
data = [['Alex',10],['Bob',12],['Clarke',13]]
df = pd.DataFrame(data,columns=['Name','Age'])
print(df)


import pandas as pd
data = [['Alex',10],['Bob',12],['Clarke',13]]
df = pd.DataFrame(data,columns=['Name','Age'],dtype=float)
print(df)


******************************************************************************************
June 7th Class
******************************************************************************************
import numpy as np
a = np.zeros(3)
a

type(a)
type(a[0])


a.shape = (10,1)
a


z = np.ones(10)
z

y = np.array([10,20])
y


from skimage import io
photo = io.imread('/Users/scher5/Desktop/google_pic2.png')

type(photo)

photo.shape

import matplotlib.pyplot as plt
plt.imshow(photo)


plt.imshow(photo[::-1])

plt.imshow(photo[50:150, 150:280])


print(sum(photo))


******************************************************************************************
******************************************************************************************
import numpy as np
import pandas as pd
file_name = '/Users/scher5/Desktop/weather_data.txt'
df = pd.read_csv(file_name)
print(df)



import numpy as np
import pandas as pd
1.
def header(msg):
    print('-' * 60)
    print('[ ' + msg + ' ]')

header("1. load hard-coded data into df")

df = pd.DataFrame(
    [['Jan', 58, 49,71, 22, 2.92],
     ['Feb', 60, 47,72, 22, 0.95],
     ['Mar', 70, 49,74, 22, 2.94],
     ['Apr', 72, 50,64, 22, 2.45],
     ['May', 75, 42,84, 22, 0.96],
     ['Jun', 76, 42,94, 22, 2.67],
     ['Jul', 78, 42,94, 22, 2.56],
     ['Aug', 79, 42,74, 22, 2.95],
     ['Sep', 70, 42,74, 22, 2.57],
     ['Oct', 65, 40,87, 22, 0.47],
     ['Nov', 60, 39,67, 22, 1.95],
     ['Dec', 50, 41,57, 22, 2.90]],
    index = [0,1,2,3,4,5,6,7,8,9,10,11],
    columns = ['month','avg_high','avg_low','rec_high','rec_low','avg_precipitation']
)
print(df)

******************************************************************************************
******************************************************************************************

2.
header("2. read text file data into df")
file_name = 'weather_data.txt'
df = pd.read_csv(file_name)
print(df)


******************************************************************************************
******************************************************************************************


3.
header("3. print head and tail records from df")
print(df.head(3))
print(df.tail(3))

******************************************************************************************
******************************************************************************************

4.
header("4. print index , types, values of df")
print(df.dtypes)

print(df.index)

print(df.columns)

print(df.values)

******************************************************************************************
******************************************************************************************

5.
header("5. save df to directory")
df.to_csv('foo.csv')



******************************************************************************************
******************************************************************************************

print('df.shape() gives number of rows,cols', df.shape)
6.
header("6. Iterate the df thru for loop")

for index, row in df.iterrows():
    print(index, row["month"],row["rec_low"],)

******************************************************************************************
******************************************************************************************

from statistics import mean
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import style

******************************************************************************************
******************************************************************************************

import matplotlib.pyplot as plt
plt.plot([1,2,3,4,0])
plt.show()
******************************************************************************************
******************************************************************************************

print('plot the graph for squares')
import matplotlib.pyplot as plt
plt.plot([1,2,3,4], [1,4,9,16], 'ro')
plt.axis([0, 6, 0, 20])
plt.show()


******************************************************************************************
******************************************************************************************

import numpy as np
import matplotlib.pyplot as plt

# evenly sampled time at 200ms intervals
t = np.arange(0., 5., 0.2)

# red dashes, blue squares and green triangles
plt.plot(t, t, 'r--', t, t**2, 'bs', t, t**3, 'g^')
