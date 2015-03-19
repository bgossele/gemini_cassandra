'''
Created on Mar 16, 2015

@author: brecht
'''
import abc
from types import UnicodeType

class Expression(object):
    
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def evaluate(self, session, starting_set):
        return

    @abc.abstractmethod
    def to_string(self):
        return

    @abc.abstractmethod
    def can_prune(self):
        return

class Simple_expression(Expression):
    
    def __init__(self, from_table, select_column, where_clause):
        self.table = from_table
        self.select_column = select_column
        self.where_clause = where_clause
        
    def evaluate(self, socket, starting_set):
        
        if starting_set == []:
            return []
        
        query = "SELECT %s FROM %s" % (self.select_column, self.table)
        if self.where_clause != "":
            query += " WHERE %s" % self.where_clause            
        if self.can_prune() and not starting_set == "*":
            if type(starting_set[0]) is UnicodeType:
                in_clause = "','".join(starting_set)            
                query += " AND %s IN ('%s')" % (self.select_column, in_clause)
            else:
                in_clause = ",".join(map(lambda x: str(x), starting_set))            
                query += " AND %s IN (%s)" % (self.select_column, in_clause)
        
        return rows_as_list(socket.execute(query))
    
    def to_string(self):
        return self.where_clause
    
    def can_prune(self):
        return not any (op in self.where_clause for op in ["<", ">"])
    
class AND_expression(Expression):
    
    def __init__(self, left, right):
        self.left = left
        self.right = right
        
    def evaluate(self, session, starting_set):
        
        if starting_set == []:
            return []
        
        if self.right.can_prune():
            temp = self.left.evaluate(session, starting_set)
            return self.right.evaluate(session, temp)
        elif self.left.can_prune():
            temp = self.right.evaluate(session, starting_set)
            return self.left.evaluate(session, temp)
        else:
            temp = self.left.evaluate(session, starting_set)
            return intersect(temp, self.right.evaluate(session, temp))
    
    def to_string(self):
        res = "(" + self.left.to_string() + ")" + " AND " + "(" + self.right.to_string() + ")"
        return res
    
    def can_prune(self):
        return True
    
class OR_expression(Expression):
    
    def __init__(self, left, right):
        self.left = left
        self.right = right
        
    def evaluate(self, session, starting_set):
        
        if starting_set == []:
            return []
        
        return union(self.left.evaluate(session, starting_set), self.right.evaluate(session, starting_set))
    
    def to_string(self):
        res = "(" + self.left.to_string() + ")" + " OR " + "(" + self.right.to_string() + ")"
        return res
    
    def can_prune(self):
        return True
    
class NOT_expression(Expression):
    
    def __init__(self, exp, table, select_column):
        self.exp = exp
        self.table = table
        self.select_column = select_column
        
    def evaluate(self, session, starting_set):
        
        if starting_set == []:
            return []        
        elif starting_set == '*':
            correct_starting_set = rows_as_list(session.execute("SELECT %s FROM %s" % (self.select_column, self.table)))
        else:
            correct_starting_set = starting_set
            
        return diff(correct_starting_set, self.exp.evaluate(session, correct_starting_set))
    
    def to_string(self):
        return "NOT (" + self.exp.to_string() + ")"
    
    def can_prune(self):
        return True

def diff(list1, list2):
    return filter(lambda x: not x in list2, list1)

def union(list1, list2):
    return set(list1 + list2)

def intersect(list1, list2):
    return filter(lambda x: x in list2, list1)
    
def rows_as_list(rows):
    return map(lambda x: x[0], rows)
    
