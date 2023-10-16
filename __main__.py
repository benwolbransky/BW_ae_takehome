import sqlite3
import pandas as pd
from glob import glob
from sample_helpers.sample_sql import sample_Tables

if __name__ == '__main__':
    c = sample_Tables()
    c.build_dbs()
    