from typing import List, Dict
import os
import json
import pandas as pd

from item.production.item_production import ProductionItem
import global_variables.path as gp


class ProductionRule:
    """Model of a ProductionRule"""
    
    __slots__ = "inputs", "output", "exp"
    
    inputs: List[ProductionItem]
    output: ProductionItem
    exp: Dict[str, int or float]
    
    def __init__(self, input: List[Dict[str, int]], output: List[Dict[str, int]], experience: Dict[str, int]):
        self.inputs = [ProductionItem(el) for el in input]
        self.output = ProductionItem(output[0])
        self.exp = experience
    
    def __str__(self):
        p = self.profit
        
        return f"""[ {", ".join([str(i) for i in self.inputs])} => {self.output} ({'+' if p > 0 else ''}{p}gp)]"""
    
    @property
    def profit(self) -> int:
        """Estimated profit for executing production with the items described in input and output"""
        return self.output.sell_price - sum([i.buy_price*i.quantity for i in self.inputs])
    
    @property
    def skill(self) -> str:
        if len(self.exp) > 0:
            return list(self.exp.keys())[0]
        else:
            return ""
    
    @property
    def csv_line(self) -> Dict[str, str | int | float]:
        line = {'skill': self.skill, 'item': self.output.item.item_name, 'price': self.output.sell_price, 'quantity': self.output.quantity, 'profit': self.profit}
        for idx, i in enumerate(self.inputs):
            line[f'input{idx+1}'] = i.item.item_name
            line[f'price{idx+1}'] = i.buy_price
            line[f'quantity{idx+1}'] = i.quantity
        return line


def json_to_df(src_dir: str = gp.dir_item_production):
    """Merge all the item production json files into a single pandas DataFrame"""
    merged = []
    
    for f in os.listdir(src_dir):
        if os.path.splitext(f)[1] != '.json':
            continue
        # f = os.path.join(src_dir, f)
        try:
            data = json.load(open(os.path.join(src_dir, f), 'r'))
        except json.JSONDecodeError as e:
            e.add_note(f"Unable to parse file: {os.path.join(src_dir, f)}")
            print(e.__notes__)
            continue
        for el in data:
            try:
                p = ProductionRule(**el)
            except KeyError:
                continue
            if p.profit > 0:
                print(p)
            merged.append(p.csv_line)
    pd.DataFrame(merged).to_csv(gp.dir_item_production+'production.csv', index=False)


def parse_production_json_file(path: str):
    """Parse the production json file located at `path`"""
    # for next_file
    ...


def parse_production_rules(src_dir: str = gp.dir_item_production):
    """Parse all production rules json files iteratively and upload them"""
    ...



if __name__ == "__main__":
    print(json_to_df())