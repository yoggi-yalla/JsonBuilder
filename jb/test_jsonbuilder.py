from jb.jsonbuilder import Tree
import rapidjson

def test_1():
    with open('jb/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/test.csv').build().toJson(indent=2)
    with open('jb/testdata/output_test_1.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output

def test_2():
    with open('jb/testdata/format2.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/test.csv').build().toJson(indent=2)
    with open('jb/testdata/output_test_2.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output

def test_3():
    with open('jb/testdata/format3.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/test.csv').build().toJson(indent=2)
    with open('jb/testdata/output_test_3.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output

def test_3_quick():
    with open('jb/testdata/format3quick.json', 'r') as f:
        output_quick = Tree(rapidjson.load(f), 'jb/testdata/test.csv').build().toJson(indent=2)
    with open('jb/testdata/format3.json', 'r') as f:
        output_slow = Tree(rapidjson.load(f), 'jb/testdata/test.csv').build().toJson(indent=2)
    assert output_quick == output_slow

def test_crif():
    with open('jb/testdata/formatcrif.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/testcrif.csv').build().toJson(indent=2)
    with open('jb/testdata/output_test_crif.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_txt():
    with open('jb/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/test.txt').build().toJson(indent=2)
    with open('jb/testdata/output_test_txt.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_pipe():
    with open('jb/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/testpipe.txt').build().toJson(indent=2)
    with open('jb/testdata/output_test_pipe.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_xlsx():
    with open('jb/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/test.xlsx').build().toJson(indent=2)
    with open('jb/testdata/output_test_xlsx.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_nan():
    with open('jb/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/testnan.csv').build().toJson(indent=2)
    with open('jb/testdata/output_test_nan.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_headeronly():
    with open('jb/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/test_headeronly.csv').build().toJson(indent=2)
    with open('jb/testdata/output_test_headeronly.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_empty():
    with open('jb/testdata/formatempty.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/test.csv').build().toJson(indent=2)
    with open('jb/testdata/output_test_empty.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_utf():
    with open('jb/testdata/format1.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/testutf.csv').build().toJson(indent=2)
    with open('jb/testdata/output_test_utf.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_full():
    with open('jb/testdata/formatfull.json', 'r') as f:
        output = Tree(rapidjson.load(f), 'jb/testdata/testfull.csv', date='2020-02-02').build().toJson(indent=2)
    with open('jb/testdata/output_test_full.json', 'r') as f:
        expected_output = f.read()
    assert output == expected_output 

def test_full_csv_excel():
    with open('jb/testdata/formatfull.json', 'r') as f:
        output1 = Tree(rapidjson.load(f), 'jb/testdata/testfull.csv', date='2020-02-02').build().toJson(indent=2)
    with open('jb/testdata/formatfull.json', 'r') as f:
        output2 = Tree(rapidjson.load(f), 'jb/testdata/testfull.xlsx', date='2020-02-02').build().toJson(indent=2)
    assert output1 == output2 

def test_full_csv_eval():
    with open('jb/testdata/formatfull.json', 'r') as f:
        output1 = Tree(rapidjson.load(f), 'jb/testdata/testfull.csv', date='2020-02-02').build().toJson(indent=2)
    with open('jb/testdata/formatfull.json', 'r') as f:
        output2 = Tree(rapidjson.load(f), 'jb/testdata/testfull.csv', date='2020-02-02', use_native_eval=True).build().toJson(indent=2)
    assert output1 == output2 

def test_full_excel_eval():
    with open('jb/testdata/formatfull.json', 'r') as f:
        output1 = Tree(rapidjson.load(f), 'jb/testdata/testfull.xlsx', date='2020-02-02').build().toJson(indent=2)
    with open('jb/testdata/formatfull.json', 'r') as f:
        output2 = Tree(rapidjson.load(f), 'jb/testdata/testfull.xlsx', date='2020-02-02', use_native_eval=True).build().toJson(indent=2)
    assert output1 == output2 
