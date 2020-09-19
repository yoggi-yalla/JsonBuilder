import json

input1 = """
{
    "trades": [
        {
            "trade_id": "com-opt-vanilla-call",
            "instruments": [
                {
                    "type": "VanillaOption",
                    "asset_1": "XAU",
                    "asset_2": "USD",
                    "put_call": "PUT",
                    "long_short": "LONG",
                    "quantity_1": 650,
                    "strike": 1000,
                    "exercise_date": "2020-01-01"
                },
                {
                    "type": "Bullet",
                    "currency": "EUR",
                    "notional": 2500,
                    "payment_date": "2020-01-01"
                }
            ]
        },
        {
            "trade_id": "com-opt-vanilla-call",
            "instruments": [
                {
                    "type": "VanillaOption",
                    "asset_1": "XAU",
                    "asset_2": "USD",
                    "put_call": "PUT",
                    "long_short": "LONG",
                    "quantity_1": 650,
                    "strike": 1000,
                    "exercise_date": "2020-01-01"
                },
                {
                    "type": "Bullet",
                    "currency": "EUR",
                    "notional": 2500,
                    "payment_date": "2020-01-01"
                }
            ]
        }
    ]
}
"""


def main():
    json1 = json.loads(input1)
    mapping = analyze(json1, {})
    fmt = {"mapping":mapping}
    output = json.dumps(fmt,indent=2)
    print(output)
    with open("../testdata/format.json", "w") as out:
        out.write(output)



def analyze(node, mapping):
    if type(node) == dict:
        mapping = analyze_obj(node, mapping)
    elif type(node) == list:
        mapping = analyze_arr(node, mapping)
    else:
        mapping = analyze_prim(node, mapping)
    return mapping

def analyze_obj(node, mapping):
    mapping["type"] = "object"
    mapping["children"] = []
    for name, value in node.items():
        child_mapping = analyze(value, {"name": name})
        mapping["children"].append(child_mapping)
    return mapping

def analyze_arr(node, mapping):
    mapping["type"] = "array"
    mapping["children"] = []
    for child in node:
        child_mapping = analyze(child, {})
        if not mapping["children"]:
            mapping["children"].append(child_mapping)
        elif not any([x == child_mapping for x in mapping["children"]]):
            mapping["children"].append(child_mapping)
    return mapping

def analyze_prim(node, mapping):
    return mapping

if __name__ == '__main__':
    main()