from pyparsing import *
import json

def plain(cmd):
  def plain_(tokens):
    c = tokens.asDict()
    c['command'] = cmd
    return c
  return plain_

make_start, make_stop, make_get, make_set, make_drop, make_delay, make_tamper, make_join = map(plain, "start stop get set drop delay tamper join".split())

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

_split = plain("split")
def make_split(tokens):
  c = _split(tokens)
  c['nodes'] = filter(len, map(str.strip, tokens.nodes.replace(" ", ",").split(",")))
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

count = Word(nums)("count").setParseAction(lambda x: int(x[0]))
# count = OneOrMore(nums)("count")

start, stop, get, set, send, drop, delay, tamper, after, split, join, to_, from_, by_ = map(lambda x: Keyword(x),
    "start stop get set send drop delay tamper after split join to from by".split())
to_ = to_("to").setParseAction(lambda x: True)
from_ = from_("from").setParseAction(lambda x: True)

cmds = Forward()

startcmd = (start + name + params).setParseAction(make_start) + EOL
stopcmd = (stop + name).setParseAction(make_stop) + EOL
setcmd = ((set + name + key + value) | (set + key + value)).setParseAction(make_set) + EOL
getcmd = ((get + name + key) | (get + key)).setParseAction(make_get) + EOL
sendcmd = (send + _json).setParseAction(make_send) + EOL
dropcmd = ((drop + count + EOL) | (drop + count + Optional(to_ | from_) + name + EOL)).setParseAction(make_drop)
delaycmd = ((delay + count + by_ + count("delay") + EOL) | (delay + count + Optional(to_ | from_) + name + by_ + count("delay") + EOL)).setParseAction(make_delay)
tampercmd = ((tamper + count + EOL) | (tamper + count + Optional(to_ | from_) + name + EOL)).setParseAction(make_tamper)
aftercmd = (after + count + LBRACE + EOL + cmds("commands") + RBRACE + EOL).setParseAction(make_after)
splitcmd = (split + name + restOfLine("nodes") + EOL).setParseAction(make_split)
joincmd = (join + name + EOL).setParseAction(make_join)

cmds << Group(OneOrMore(startcmd | stopcmd | getcmd | setcmd | sendcmd | dropcmd | delaycmd | tampercmd | aftercmd | splitcmd | joincmd)).setWhitespaceChars(" \t")


cmds.verbose_stacktrace = True
cmds.ignore(comment)

def parse(string=None, filename=None):
  if filename is not None:
    return cmds.parseFile(filename, parseAll=True)[0].asList()
  elif string is not None:
    return cmds.parseString(string)[0].asList()
  else:
    raise Exception("No file or string given")

