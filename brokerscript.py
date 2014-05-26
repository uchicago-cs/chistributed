from pyparsing import *
import json

def make_start(tokens):
  return {'command': 'start', 'name': tokens.name, 'params': tokens.params}

def make_stop(tokens):
  return {'command': 'stop', 'name': tokens.name}

def make_get(tokens):
  cmd = {'command': 'get', 'key': tokens.key}
  if len(tokens.name):
    cmd['name'] = tokens.name
  return cmd

def make_set(tokens):
  cmd = {'command': 'set', 'key': tokens.key, 'value': tokens.value}
  if len(tokens.name):
    cmd['name'] = tokens.name
  return cmd

def make_send(tokens):
  return {'command': 'json', 'json': json.loads(tokens.json.strip())}

def parse(string=None, filename=None):
  ws = ' \t'
  ParserElement.setDefaultWhitespaceChars(ws)
  ParserElement.verbose_stacktrace = True

  EOL = LineEnd().suppress()
  comment = (LineStart() + "#" + restOfLine + EOL) | ("#" + restOfLine)

  name, key, value = map(lambda x: Word(alphanums)(x),
      ["name", "key", "value"])

  params = restOfLine("params")
  json = restOfLine("json")

  start, stop, get, set, send = map(lambda x: Keyword(x),
      ["start", "stop", "get", "set", "send"])

  startcmd = (start + name + params).setParseAction(make_start)
  stopcmd = (stop + name).setParseAction(make_stop)
  setcmd = ((set + name + key + value) | (set + key + value)).setParseAction(make_set)
  getcmd = ((get + name + key) | (get + key)).setParseAction(make_get)
  sendcmd = (send + json).setParseAction(make_send)

  cmds = Group(OneOrMore((startcmd | stopcmd | getcmd | setcmd | sendcmd) + EOL)).setWhitespaceChars(" \t")
  cmds.verbose_stacktrace = True
  cmds.ignore(comment)

  if filename is not None:
    return cmds.parseFile(filename, parseAll=False)[0].asList()
  elif string is not None:
    return cmds.parseString(string)[0].asList()
  else:
    raise Exception("No file or string given")

