from ebay_ops import *
from immonet_ops import *
from immowelt_ops import *

from data_ops import *

from loguru import logger

ALL_TABLES = ["immonet"]

logger.add("logs/main.log", format="{time} {level} {message}", level="DEBUG")
f = open('logs/main.log', 'w')
f.close()


def Drop_table(table_name):
    cursor.execute("DROP TABLE " + table_name + ";")
    conn.commit()
    logger.info('The table \"' + table_name + '\" was dropped')

for table_name in ALL_TABLES:
    logger.info('''
    
    
# -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_ # Table "''' + table_name + '''" # -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_ #''')
    Drop_table(table_name)
    Table_crate(table_name)
    Table_copying(table_name)
    Table_cleaning(table_name)
    Table_fill(table_name)

cursor.close()
conn.close()

logger.info('''


# -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_ # End of the program # -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_ #''')
