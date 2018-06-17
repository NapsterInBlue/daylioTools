from etl.createTables import (build_empty_tables,
                                          build_basic_dim_tables)
from etl.getRawData import get_raw_data
from etl.loadCleanData import load_clean_data

def update_data():
    build_empty_tables()
    build_basic_dim_tables()
    get_raw_data()
    load_clean_data()


if __name__ == "__main__":
    build_empty_tables()
    build_basic_dim_tables()
    get_raw_data()
    load_clean_data()
