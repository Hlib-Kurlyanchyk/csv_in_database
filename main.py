from ebay_ops import *
from immonet_ops import *
from immowelt_ops import *
from loguru import logger

logger.add("logs/main.log", format="{time} {level} {message}", level="DEBUG")
f = open('logs/main.log', 'w')
f.close()


def DROP_TABLE(table_name):
    cursor.execute("DROP TABLE " + table_name + ";")
    conn.commit()
    logger.info('The table \"' + table_name + '\" was dropped')


logger.info('# --------------------------- # Tabele "immowelt" # ---------------------------- #')
DROP_TABLE("immowelt")
TABLE_IMMOWELT_CREATE()
TABLE_IMMOWELT_DATA_COPYING()
TABLE_IMMOWELT_DATA_CLEANING()
TABLE_IMMOWELT_DATA_FILL()


logger.info('# ------------------------------ # Table "ebay" # ------------------------------ #')
DROP_TABLE("ebay")
Table_ebay_crate()
Table_ebay_copying()
Table_ebay_cleaning()
Table_ebay_fill()

logger.info('# ---------------------------- # Table "immonet" # ----------------------------- #')
DROP_TABLE("immonet")
Table_immonet_crate()
Table_immonet_copying()
Table_immonet_cleaning()
Table_immonet_fill()

cursor.close()
conn.close()

logger.info('# ------------------------------- # END OF PROGRAM # --------------------------- #')
