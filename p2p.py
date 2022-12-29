from os.path import isdir
from pathlib import Path

starting_path = "Y://Retention//All//Retention All//"
if not Path(starting_path).exists():
    starting_path = "Z://Retention//All//Retention All//"
    if not Path(starting_path).exists():
        starting_path = "//srvm-totofs//TTKC//Retention//All//Retention All//"
        if not Path(starting_path).exists():
            starting_path = "Y://"
            if not Path(starting_path).exists():
                starting_path = "Z://"

import sys

sys.path.append(f'{starting_path}with_python//python_libraries//')
import OOP

for bonus_type in OOP.P2P.all_types:
    if len(bonus_type) > 0:
        for action in bonus_type:
            engine = OOP.P2P(action)
            if engine.check_validity():
                raw_data = engine.get_data()
                if type(raw_data) == tuple:
                    deposits, bets = raw_data
                    prp_data = engine.preprocessing(bets=bets, deposit=deposits)
                else:
                    prp_data = engine.preprocessing(bets=raw_data)
                engine.make_excels(prp_data=prp_data)
            else:
                continue
