from src.jsonbuilder import Tree
import rapidjson

def test_1():
    with open('tests/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/test.csv').build().toJson(indent=2)
    with open('tests/testdata/output_test_1.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output

def test_2():
    with open('tests/testdata/format2.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/test.csv').build().toJson(indent=2)
    with open('tests/testdata/output_test_2.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output

def test_3():
    with open('tests/testdata/format3.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/test.csv').build().toJson(indent=2)
    with open('tests/testdata/output_test_3.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output

def test_3_quick():
    with open('tests/testdata/format3quick.json', 'r') as f:
        output_quick = Tree(rapidjson.load(f), 'tests/testdata/test.csv').build().toJson(indent=2)
    with open('tests/testdata/format3.json', 'r') as f:
        output_slow = Tree(rapidjson.load(f), 'tests/testdata/test.csv').build().toJson(indent=2)
    assert output_quick == output_slow

def test_crif():
    with open('tests/testdata/formatcrif.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/testcrif.csv').build().toJson(indent=2)
    with open('tests/testdata/output_test_crif.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_txt():
    with open('tests/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/test.txt').build().toJson(indent=2)
    with open('tests/testdata/output_test_txt.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_pipe():
    with open('tests/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/testpipe.txt').build().toJson(indent=2)
    with open('tests/testdata/output_test_pipe.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_xlsx():
    with open('tests/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/test.xlsx').build().toJson(indent=2)
    with open('tests/testdata/output_test_xlsx.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_nan():
    with open('tests/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/testnan.csv').build().toJson(indent=2)
    with open('tests/testdata/output_test_nan.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_headeronly():
    with open('tests/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/test_headeronly.csv').build().toJson(indent=2)
    with open('tests/testdata/output_test_headeronly.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_empty():
    with open('tests/testdata/formatempty.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/test.csv').build().toJson(indent=2)
    with open('tests/testdata/output_test_empty.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_utf():
    with open('tests/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'tests/testdata/testutf.csv').build().toJson(indent=2)
    with open('tests/testdata/output_test_utf.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 
