import sys
from gemini.gemini_constants import HET, HOM_ALT, HOM_REF, UNKNOWN
from query_expressions import Simple_expression, NOT_expression, AND_expression, OR_expression


'''
Created on Mar 18, 2015

@author: brecht
'''
def _get_matching_sample_ids(session, wildcard):
    
    query = 'SELECT name FROM samples '
    if wildcard.strip() != "*":
        query += ' WHERE ' + wildcard

    sample_info = []  # list of sample_id/name tuples
    for row in session.execute(query):
        sample_info.append(row[0])
    return sample_info

def _swap_genotype_for_number(token):
            
    if any(g in token for g in ['HET', 'HOM_ALT', 'HOM_REF', 'UNKNOWN']):
        token = token.replace('HET', str(HET))
        token = token.replace('HOM_ALT', str(HOM_ALT))
        token = token.replace('HOM_REF', str(HOM_REF))
        token = token.replace('UNKNOWN', str(UNKNOWN))
    return token
        
def gt_filter_to_query_exp(gt_filter):
    i = -1
    operators = ['!=', '<=', '>=', '=', '<', '>']
    for op in operators:
        temp = gt_filter.find(op)
        if temp > -1:
            i = temp
            break
                
    if i > -1:
        left = gt_filter[0:i].strip()
        clause = _swap_genotype_for_number(gt_filter[i:].strip())
    else:
        sys.exit("ERROR: invalid --gt-filter command 858.")
            
    not_exp = False
    if clause.startswith('!'):
        not_exp = True
        clause = clause[1:]
                    
    column = left.split('.', 1)[0]
    sample = left.split('.', 1)[1]
            
    exp = Simple_expression('variants_by_samples_' + column, 'variant_id' , "sample_name = '" + sample + "' AND " + column + clause)
    if not_exp:
        return NOT_expression(exp, 'variants', 'variant_id')
    else:
        return exp
    
def fold(function, iterable, initializer=None):
    it = iter(iterable)
    if initializer is None:
        try:
            initializer = next(it)
        except StopIteration:
            raise TypeError('reduce() of empty sequence with no initial value')
    accum_value = initializer
    for x in it:
        accum_value = function(x, accum_value)
    return accum_value

def parse_wildcard(token, session):
    if token.count(';') != 3:
        sys.exit("Wildcard filter should consist of 4 elements. Exiting.")

    (column, wildcard, wildcard_rule, wildcard_op) = token.split(';')
    column = column.strip()
    wildcard = wildcard.strip()
    wildcard_rule = wildcard_rule.strip()
    wildcard_op = wildcard_op.strip()
                
    sample_info = _get_matching_sample_ids(session, wildcard)

                # Replace HET, etc. with 1, et.session to avoid eval() issues.
    wildcard_rule = _swap_genotype_for_number(wildcard_rule)
    wildcard_rule = wildcard_rule.replace('==', '=')
    rule = None
                
    def sample_to_expr(sample):
                    
        corrected = False
        if wildcard_rule.startswith('!'):
            corrected = True
            corrected_rule = wildcard_rule[1:]
        else:
            corrected_rule = wildcard_rule
                        
        expr = Simple_expression('variants_by_samples_' + column, 'variant_id', \
                            "sample_name = '" + sample + \
                            "' AND " + column + corrected_rule)
        if corrected:
            return NOT_expression(expr, 'variants', 'variant_id')
        else:
            return expr
                    
    rules = map(sample_to_expr, sample_info)
                
    # build the rule based on the wildcard the user has supplied.
    if wildcard_op == "all":
                    
        if len(rules) > 0:
            rule = fold(lambda l,r: AND_expression(l,r), rules[1:], rules[0])
                        
    elif wildcard_op == "any":
                    
        if len(rules) > 0:
            rule = fold(lambda l,r: OR_expression(l,r), rules[1:], rules[0])

    elif wildcard_op == "none":
                    
        rules = map(lambda exp: NOT_expression(exp, 'variants', 'variant_id'), rules)
        if len(rules) > 0:
            rule = fold(lambda l,r: AND_expression(l,r), rules[1:], rules[0])

    elif "count" in wildcard_op:
        sys.exit("Not yet implemented. Exiting." % wildcard_op)
                    
    else:
        sys.exit("Unsupported wildcard operation: (%s). Exiting." % wildcard_op)
                
    return rule
            
def parse_gt_filter(gt_filter):
    
    gt_filter = gt_filter.strip()    
    depth = 0
    min_depth = 100000 #Arbitrary bound on nr of nested clauses.
    
    for i in range(0,len(gt_filter)):
        if gt_filter[i] == '(':
            depth += 1
        elif gt_filter[i] == ')':
            depth -= 1
        elif i < len(gt_filter) - 2:
            if gt_filter[i:i+2] == "OR":
                    if depth == 0:
                        left = parse_gt_filter(gt_filter[:i].strip())
                        right = parse_gt_filter(gt_filter[i+2:].strip())
                        return OR_expression(left, right)
                    else:
                        min_depth = min(min_depth, depth)
                
            elif i < len(gt_filter) - 3:
                if gt_filter[i:i+3] == "AND":
                    if depth == 0:
                        left = parse_gt_filter(gt_filter[:i].strip())
                        right = parse_gt_filter(gt_filter[i+3:].strip())
                        return AND_expression(left, right)
                    else:
                        min_depth = min(min_depth, depth)
                elif gt_filter[i:i+3] == "NOT":
                    if depth == 0:
                        body = parse_gt_filter(gt_filter[i+3:].strip())
                        return NOT_expression(body, 'variants', 'variant_id')
                    else:
                        min_depth = min(min_depth, depth)
    if depth == 0:
        if min_depth < 100000:
            return parse_gt_filter(gt_filter[min_depth:len(gt_filter)-min_depth])
        else:
            token = gt_filter
            if (token.find("gt") >= 0 or token.find("GT") >= 0) and not ';' in token:
                return gt_filter_to_query_exp(token)
            elif (token.find("gt") >= 0 or token.find("GT") >= 0) and ';' in token:
                print "wildcard found"
                return parse_wildcard(token)
            else:
                sys.exit("ERROR: invalid --gt-filter command")           
    else:
        sys.exit("ERROR in gt-filter. Brackets don't match")
        