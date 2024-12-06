

import os
import time
import pandas as pd
import subprocess


# Task scheduler 
# loc: C:\Users\hello\AppData\Local\Programs\Python\Python311\python.exe 
# args: C:\Users\hello\Documents\GitHub\eq-sys-2\yf-reduced\run_all_assets_v4_reduced.py
npx_path = r'"C:\Program Files\nodejs\npx.cmd"'
node_path = r"C:\Program Files\nodejs"
npx_path_duka = r'"C:\Users\hello\node_modules\.bin\dukascopy-node.cmd"'
# symbol_path = r"C:\Users\hello\Documents\GitHub\eq-sys-2\duka\all_duka_id.txt"
symbol_path = r"C:\Users\hello\Documents\GitHub\eq-sys-2\duka\duka_id_abi.txt"
download_location = r'"C:\Users\hello\Documents\GitHub\eq-sys-2\duka\dukascope-downloads"'

symbols = []
# Define a list of commands to run
print(symbol_path)
with open(symbol_path, 'r') as file:
    for s in file:
        symbols.append(s.strip())
    
# symb

for symbol in symbols:
    cmd = f"{npx_path} {npx_path_duka} -i {symbol} -from 1980-06-01 -to 2023-05-01 -t m1 -bs 15 -bp 5000 -dir {download_location}"
    print(f'executing {npx_path} {npx_path_duka} -i {symbol} -from 1980-06-01 -to 2023-05-01 -t m1 -bs 15 -bp 5000 -dir {download_location}'
)
    subprocess.run(cmd, shell=True, cwd=node_path)