from ebay_ops import *
from immonet_ops import *
from immowelt_ops import *
from immowelt_data_cleaning import *
import pytest

from loguru import logger

logger.add("logs/main.log", format="{time} {level} {message}", level="DEBUG")
f = open('logs/main.log', 'w')
f.close()


def DROP_TABLE(table_name):
    cursor.execute("DROP TABLE " + table_name + ";")
    conn.commit()
    logger.info('The table \"' + table_name + '\" was dropped')


logger.info('# --------------------------- # Tabele "immowelt" # ---------------------------- #')
DATA_IMMOWELT_COLLECTION()
DROP_TABLE("immowelt")
TABLE_IMMOWELT_CREATE()
TABLE_IMMOWELT_FILL(100)

# logger.info('# ------------------------------ # Table "ebay" # ------------------------------ #')
# DROP_TABLE("ebay")
# TABLE_EBAY_CREATE()
# TABLE_EBAY_FILL(0)

# logger.info('# ---------------------------- # Table "immonet" # ----------------------------- #')
# DROP_TABLE("immonet")
# TABLE_IMMONET_CREATE()
# TABLE_IMMONET_FILL(0)


cursor.close()
conn.close()

logger.info('# ------------------------------- # END OF PROGRAM # --------------------------- #')
