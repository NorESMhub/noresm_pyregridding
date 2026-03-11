import csv
import yaml
import re

# ── configuration ──────────────────────────────────────────
INPUT_CSV  = "data.csv"
OUTPUT_YAML = "data.yaml"

KEEP_COLUMNS = ["Branded Variable Name",
                "CMIP6 Compound Name",
                "Description",
                "Modelling Realm-Primary",
                "Dimensions",
                "NorESM3 name (dependency)", 
                "Units (from Physical Parameter)",
                "CMIP7 Freq." ]

class InlineListDumper(yaml.Dumper):
    pass

def inline_list_representer(dumper, data):
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=True)

def should_keep(row):
    """Return True if this row should be included in the output."""
    keep_row = False
    if row["Modelling Realm-Primary"] == 'atmos':
        keep_row = True
    elif row["Modelling Realm-Primary"] == 'land':
        keep_row = True
    if keep_row:
        if not row["NorESM3 name (dependency)"].strip():
            keep_row = False
        if "?" in row["NorESM3 name (dependency)"]:
            keep_row = False
        if "n/a" in row["NorESM3 name (dependency)"]: 
            keep_row = False
        if "IN SURF DATASET" in row["NorESM3 name (dependency)"]:
            keep_row = False
    return keep_row


def is_math_expression(expr: str) -> bool:
    """
    Return True if expr is a mathematical expression,
    Return False if it is a single variable name.

    >>> is_math_expression("PRECC + PRECL")
    True
    >>> is_math_expression("PRECC * 0.001")
    True
    >>> is_math_expression("verticalsum(SOILWATER)")
    True
    >>> is_math_expression("PRECC")
    False
    >>> is_math_expression("T2M")
    False
    """
    expr = expr.strip()

    if re.search(r'[+\-*/^%]', expr):
        return True
    if re.search(r'\w+\s*\(', expr):
        return True
    if re.search(r'\d', expr):
        return True
    if re.search(r'\w+\s+\w+', expr):
        return True

    return False


def extract_variables(expr: str) -> list:
    """
    Extract variable names from a mathematical expression,
    ignoring operators, numbers, and known functions/modules.

    >>> extract_variables("PRECC + PRECL")
    ['PRECC', 'PRECL']
    >>> extract_variables("PRECC * 0.001")
    ['PRECC']
    >>> extract_variables("verticalsum(SOILWATER, capped_at=1000)")
    ['SOILWATER']
    >>> extract_variables("np.sqrt(U**2 + V**2)")
    ['U', 'V']
    """

    # known functions and modules to ignore
    ignore = {
        "np", "xr", "math",                           # modules
        "sqrt", "abs", "sum", "min", "max", "mean",   # common functions
        "verticalsum", "where", "zeros", "ones",      # custom/xarray functions
        "True", "False", "None",                      # python keywords
        "dim", "capped_at", "skipna",                 # common keyword arguments
    }

    # find all words in the expression
    all_words = re.findall(r'[a-zA-Z_]\w*', expr)

    # filter out ignored words and pure numbers
    variables = []
    word_dict = {}
    for word in all_words:
        if word not in ignore:
            word_dict["model_var"] = word
            variables.append(word_dict)

    return variables


def analyse_expression(expr: str) -> dict:
    """
    Analyse an expression and return whether it is a math
    expression and what variables it contains.

    >>> analyse_expression("PRECC + PRECL")
    {'is_math': True, 'variables': ['PRECC', 'PRECL']}
    >>> analyse_expression("T2M")
    {'is_math': False, 'variables': ['T2M']}
    """
    expr = expr.strip()

    if not is_math_expression(expr):
        single_var = {}
        single_var["model_var"] = expr
        return {
            "is_math": False,
            "variables": single_var   # single variable is the expression itself
        }
    return {
        "is_math": True,
        "variables": extract_variables(expr)
    }


def strip_quotes(obj):
    """Recursively strip quotes from all string values in a dict."""
    if isinstance(obj, dict):
        return {k: strip_quotes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [strip_quotes(i) for i in obj]
    if isinstance(obj, str):
        return obj.strip("'\"")
    return obj


# ── read csv ───────────────────────────────────────────────
def read_csv(filepath):
    """Read CSV and return filtered rows as a dictionary."""
    data = {}

    with open(filepath, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:

            # check if row should be kept
            if not should_keep(row):
                continue

            # build entry with only the columns we want
            entry = {}
            for col in KEEP_COLUMNS:
                if col == "Branded Variable Name":
                    continue           # used as the key, not stored inside
                if col not in row:
                    continue           # skip columns that don't exist in the csv
                value = row[col].strip()
                if value:              # only add if not blank
                    if "Modelling Realm-Primary" in col:
                        entry["table"] = value
                    elif "CMIP6 Compound Name" in col:
                        entry["long_name"] = value
                    elif "Description" in col:
                        entry["description"] = value
                    elif "Units (from Physical Parameter)" in col:
                        entry["units"] = value
                    elif "Dimensions" in col:
                        entry["dim"] = value.split(',')
                        if "lev" in value:
                            levels = {}
                            levels["name"] = "standard_hybrid_sigma"
                            levels["units"] = "1"
                            levels["src_axis_name"] = "lev"
                            levels["src_axis_bnds"] = "ilev"
                            entry["levels"] = levels
                    elif "NorESM3 name (dependency)" in col:
                        result = analyse_expression(value)
                        if result["is_math"]:
                            entry["formula"] = value
                            entry["sources"] = result["variables"]
                        else:
                            entry["sources"] = result["variables"]
                    elif "Freq" in col:
                        entry["freq"] = value

            # store entry (a dictionary) keyed by name
            name = row["Branded Variable Name"].strip()
            data[name] = entry

    return data


# ── write yaml ─────────────────────────────────────────────
def write_yaml(data, filepath):
    """Write dictionary to a YAML file."""
    with open(filepath, "w") as f:
        yaml.dump(data, f, Dumper=InlineListDumper, default_flow_style=False)


# ── main ───────────────────────────────────────────────────
if __name__ == "__main__":
    InlineListDumper.add_representer(list, inline_list_representer)
    data = read_csv(INPUT_CSV)
    write_yaml(data, OUTPUT_YAML)
    print(f"wrote {len(data)} entries to {OUTPUT_YAML}")
