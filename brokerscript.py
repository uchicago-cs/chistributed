from pyparsing import *
import json

def plain(cmd):
  def plain_(tokens):
    c = tokens.asDict()
    c['command'] = cmd
    return c
  return plain_

make_start, make_stop, make_get, make_set, make_drop, make_tamper = map(plain, "start stop get set drop tamper".split())

_send = plain("send")
def make_send(tokens):
  c = _send(tokens)
  c['json'] = json.loads(c['json'].strip())
  return c

_after = plain("after")
def make_after(tokens):
  c = _after(tokens)
  c['commands'] = tokens.commands.asList()
  return c


## Grammar
ws = ' \t'
ParserElement.setDefaultWhitespaceChars(ws)
ParserElement.verbose_stacktrace = True

EOL = LineEnd().suppress()
comment = (LineStart() + "#" + restOfLine + EOL) | ("#" + restOfLine)
LBRACE, RBRACE, = map(lambda x: Literal(x).suppress(), "{}")

name, key, value = map(lambda x: Word(alphanums)(x),
    ["name", "key", "value"])

params = restOfLine("params")
_json = restOfLine("json")

count = Word(nums)("count")

start, stop, get, set, send, drop, tamper, after, to_, from_ = map(lambda x: Keyword(x),
    "start stop get set send drop tamper after to from".split())

cmds = Forward()

startcmd = (start + name + params).setParseAction(make_start) + EOL
stopcmd = (stop + name).setParseAction(make_stop) + EOL
setcmd = ((set + name + key + value) | (set + key + value)).setParseAction(make_set) + EOL
getcmd = ((get + name + key) | (get + key)).setParseAction(make_get) + EOL
sendcmd = (send + _json).setParseAction(make_send) + EOL
dropcmd = ((drop + count + EOL) | (drop + count + (to_ | from_) + name + EOL)).setParseAction(make_drop)
tampercmd = ((tamper + count + EOL) | (tamper + count + (to_ | from_) + name + EOL)).setParseAction(make_tamper)
aftercmd = (after + count + LBRACE + EOL + cmds("commands") + RBRACE + EOL).setParseAction(make_after)

cmds << Group(OneOrMore(startcmd | stopcmd | getcmd | setcmd | sendcmd | dropcmd | tampercmd | aftercmd)).setWhitespaceChars(" \t")


cmds.verbose_stacktrace = True
cmds.ignore(comment)

def parse(string=None, filename=None):
  if filename is not None:
    return cmds.parseFile(filename, parseAll=True)[0].asList()
  elif string is not None:
    return cmds.parseString(string)[0].asList()
  else:
    raise Exception("No file or string given")

