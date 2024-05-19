import ast
import operator
import re  # for math operators
import numpy
import pandas

SAFE_TYPES = (int, float, str, list, set, map, tuple, dict, type)  # Basic types
SAFE_OPERATORS = [getattr(operator, op) for op in dir(operator) if not op.startswith('_') and op != 'not_']  # Math operators
SAFE_FUNCTIONS = {
  'numpy': ['sin', 'cos', 'tan', 'log', 'exp', 'sqrt', 'mean', 'median', 'std', 'var', 'sum', 'cumsum', 'min', 'max', 'abs', 'round', 'floor', 'ceil', 'clip', 'where', 'concatenate', 'stack', 'hstack', 'vstack', 'split', 'array', 'zeros', 'ones', 'full', 'empty', 'arange', 'linspace', 'logspace', 'eye', 'diag', 'tril', 'triu', 'identity', 'dot', 'matmul', 'tensordot', 'einsum'],
  'pandas': [
    'DataFrame', 'Series',
    # 'info', 'head', 'tail', 'describe', 'shape', 'columns', 'index', 'values', 'dtypes', 'astype',
    # 'copy', 'drop', 'dropna', 'fillna', 'interpolate', 'replace', 'apply', 'applymap', 'map', 'groupby',
    # 'agg', 'transform', 'pivot', 'pivot_table', 'melt', 'merge', 'join', 'concat', 'append', 'sort_values', 'sort_index',
    # 'set_index', 'reset_index', 'loc', 'iloc', 'at', 'iat', 'sum', 'mean', 'median', 'std', 'var',
    # 'count', 'nunique', 'unique', 'value_counts', 'isna', 'isnull', 'notna', 'notnull', 'eq', 'ne', 'lt', 'le', 'gt', 'ge'
  ]
}
BASE_NAMESPACE = {op.__name__: op for op in SAFE_OPERATORS}
BASE_NAMESPACE.update({name: getattr(module, func) for module in [numpy, pandas] for name, func in [(f, f) for f in SAFE_FUNCTIONS[module.__name__]]})
BASE_NAMESPACE.update({t.__name__: t for t in SAFE_TYPES})
# BASE_NAMESPACE.update({'numpy': numpy, 'pd': pandas}) # Add numpy and pd modules themselves
SAFE_EXPR_CACHE = set()

def safe_eval(expr, lambda_check=False, callable_check=False, **kwargs):
  """
  Evaluate a string expression in a safe environment.

  :param expr: string expression to evaluate
  :param **kwargs: additional variables to inject into the evaluation namespace
  :return: result of the evaluation
  """

  if type(expr) is not str:
    if type(expr) is callable:
      return expr
    raise ValueError("Expression must be a string")

  ns = BASE_NAMESPACE.copy()
  ns.update(kwargs)

  try:
    match = re.match(r'^\s*(def\s+\w+\s*\(|lambda\s+)', expr)
    should_be_func = match and match.group(1).startswith('def')
    should_be_lambda = match and match.group(1).startswith('lambda')

    tree = ast.parse(expr, mode='exec' if should_be_func else 'eval')
    is_func = should_be_func and isinstance(tree.body[0], ast.FunctionDef)
    is_lambda = should_be_lambda and isinstance(tree.body, ast.Lambda)

    if lambda_check and not is_lambda:
      raise ValueError("Expression must be a lambda")
    if callable_check and not (is_func or is_lambda):
      raise ValueError("Expression must be callable")

    # basic AST safety analysis
    if not expr in SAFE_EXPR_CACHE and not is_ast_safe(tree):
      raise ValueError("Invalid or unsafe expression")

    # compile the AST and evaluate
    if is_func:
      code = compile(tree, filename='<ast>', mode='exec')
      func_name = tree.body[0].name if is_func else None
      exec(code, ns)
      result = ns[func_name] # function reference to be used as lambda
    else:
      code = compile(tree, filename='<ast>', mode='eval')
      result = eval(code, ns)

    # cache the expression as safe for faster evals
    SAFE_EXPR_CACHE.add(expr)

    return result

  except Exception as e:
    raise ValueError("Error evaluating expression: {}".format(e))

def is_ast_safe(tree, allowed_functions=SAFE_FUNCTIONS):
  """
  Basic AST safety analysis with granular function allowance

  :param tree: AST node
  :param allowed_functions: dictionary of allowed functions per module
  :return: True if the AST is safe, False otherwise
  """
  # if isinstance(tree, ast.Expr):
  #   return is_ast_safe(tree.value, allowed_functions)
  # elif isinstance(tree, ast.Call):
  #   # Check if the function call is to an allowed function within a allowed module
  #   if isinstance(tree.func, ast.Attribute):
  #     # Function call on an attribute (e.g., pd.DataFrame.head)
  #     module_name = tree.func.value.id
  #     func_name = tree.func.attr
  #     return module_name in allowed_functions and func_name in allowed_functions[module_name]
  #   elif isinstance(tree.func, ast.Name):
  #     # Direct function call (e.g., sin)
  #     func_name = tree.func.id
  #     return any(module_name in allowed_functions and func_name in allowed_functions[module_name] for module_name in allowed_functions)
  #   return False  # Disallow calls to other types of functions
  # elif isinstance(tree, ast.Name):
  #   # Disallow access to system variables regardless of namespace exclusions
  #   if tree.id in ['__import__', '__builtins__', '__globals__', '__locals__']:
  #     return False
  #   return True
  # elif isinstance(tree, ast.Num):
  #   return True
  # else:
  #   return False
  return True

def safe_eval_to_lambda(expr, **kwargs):
  """
  Evaluate a string expression as a lambda function in a safe environment.

  :param expr: string expression to evaluate
  :param **kwargs: additional variables to inject into the evaluation namespace
  :return: lambda function
  """
  return lambda **kwargs: safe_eval(expr, **kwargs)

