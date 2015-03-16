'''
Created on Mar 16, 2015

@author: brecht
'''

class Expression(object):
    
    def __init__(self, from_table, select_column, where_columns, where_exps):
        self.table = from_table
        self.select_column = select_column
        self.where_columns = where_columns
        self.where_exps = where_exps
        
    def evaluate(self, session, starting_set):
        where_clause = " AND ".join(zipwith(lambda x, y: x + y, self.where_columns, self.where_exps))
        query = "SELECT %s FROM %s WHERE %s" % (self.select_column, self.table, where_clause)
        
        if not starting_set == "*":
            in_clause = ",".join(map(lambda x: str(x), starting_set))            
            query += " AND %s IN (%s)" % (self.select_column, in_clause)
            
        print query
        rows = session.execute(query)
        return rows_as_list(rows)
    
class AND_expression(object):
    
    def __init__(self, left, right):
        self.left = left
        self.right = right
        
    def evaluate(self, session, starting_set):
        temp = self.left.evaluate(session, starting_set)
        return self.right.evaluate(session, temp)
    
class OR_expression(object):
    
    def __init__(self, left, right):
        self.left = left
        self.right = right
        
    def evaluate(self, session, starting_set):
        return union(self.left.evaluate(session, starting_set), self.right.evaluate(session, starting_set))
    
class NOT_expression(object):
    
    def __init__(self, exp, table, select_column):
        self.exp = exp
        self.table = table
        self.select_column = select_column
        
    def evaluate(self, session, starting_set):
        if starting_set == '*':
            correct_starting_set = rows_as_list(session.execute("SELECT %s FROM %s" % (self.select_column, self.table)))
        else:
            correct_starting_set = starting_set
            
        return diff(correct_starting_set, self.exp.evaluate(session, correct_starting_set))
    
def zipwith(f, list_a, list_b):
    return [ f(a,b) for (a,b) in zip(list_a,list_b) ]

def diff(list1, list2):
    return filter(lambda x: not x in list2, list1)

def union(list1, list2):
    return set(list1 + list2)
    
def rows_as_list(rows):
    return map(lambda x: x[0], rows)
    
