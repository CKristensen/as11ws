import unittest
import case
import pandas as pd

#UNIT TEST
class SimplisticTest(unittest.TestCase):
    def test_handle_tuple(self):
        self.assertTrue(case.handle_tuples((1,2),3) == (1,2,3))

    def test_handle_tuple2(self):
        self.assertTrue(case.handle_tuples(1,3) == (1,3))

    def test_handle_appends(self):  
        appended = [1]
        case.handle_appends(appended, [3,4,5])
        self.assertTrue(appended == [1,3,4,5])

    def test_handle_appends2(self):  
        appended = [1,3]
        case.handle_appends(appended, 5)
        self.assertTrue(appended == [1,3,5])

    def test_get_values(self):  
        the_dic = {'Name':'Bond', 'Second' : {'Second': 'James', 'Third' : {'Third': 'Bond'}}}
        name = case.get_values(the_dic)
        self.assertTrue(name == ['Bond', 'James', 'Bond'])
    
    def test_get_values2(self):  
        the_dic = 'James'
        name = case.get_values(the_dic)
        self.assertTrue(name == ['James'])
        
    def test_get_columns(self):  
        the_dic = {'Name': 'Bond', 'Second' : {'Second': 'James', 'Third' : {'Third': 'Bond'}}}
        name = case.get_columns(the_dic)
        self.assertTrue(name == ['Name', ('Second', 'Second'), ('Second', 'Third', 'Third')])

    def test_get_columns2(self):  
       the_dic = {'Name': 'Bond', 'Second' : 'James', 'Third' : 'Bond'}
       name = case.get_columns(the_dic)
       self.assertTrue(name == ['Name', 'Second',  'Third'])

    def test_create_column_name(self):
        self.assertTrue(case.create_column_name(('Second', 'Third', 'Third')) == 'Second_Third_Third')

    def test_create_column_name2(self):
        self.assertTrue(case.create_column_name(('Second')) == 'Second')


if __name__ == '__main__':
    unittest.main()