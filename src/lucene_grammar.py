#
# lucene_grammar.py
#
# Copyright 2011, Paul McGuire
#
# implementation of Lucene grammar, as decribed
# at http://svn.apache.org/viewvc/lucene/dev/trunk/lucene/docs/queryparsersyntax.html
#

import json
import pyparsing as pp
from pyparsing import pyparsing_common as ppc


pp.ParserElement.enablePackrat()

COLON, LBRACK, RBRACK, LBRACE, RBRACE, TILDE, CARAT = map(pp.Literal, ":[]{}~^")
LPAR, RPAR = map(pp.Suppress, "()")
and_, or_, not_, to_ = map(pp.CaselessKeyword, "AND OR NOT TO".split())
keyword = and_ | or_ | not_ | to_

expression = pp.Forward()

valid_word = pp.Regex(
    r'([a-zA-Z0-9*_+.-]|\\\\|\\([+\-!(){}\[\]^"~*?:]|\|\||&&))+'
).setName("word")
valid_word.setParseAction(
    lambda t: t[0].replace("\\\\", chr(127)).replace("\\", "").replace(chr(127), "\\")
)

string = pp.QuotedString('"')

required_modifier = pp.Literal("+")("required")
prohibit_modifier = pp.Literal("-")("prohibit")
integer = ppc.integer()
proximity_modifier = pp.Group(TILDE + integer("proximity"))
number = ppc.fnumber()
fuzzy_modifier = TILDE + pp.Optional(number, default=0.5)("fuzzy")

term = pp.Forward()
field_name = valid_word().setName("fieldname")
incl_range_search = pp.Group(LBRACK - term("lower") + to_ + term("upper") + RBRACK)
excl_range_search = pp.Group(LBRACE - term("lower") + to_ + term("upper") + RBRACE)
range_search = incl_range_search("incl_range") | excl_range_search("excl_range")
boost = CARAT - number("boost")

string_expr = pp.Group(string + proximity_modifier) | string
word_expr = pp.Group(valid_word + fuzzy_modifier) | valid_word
term << (
    pp.Optional(field_name("field") + COLON)
    + (word_expr | string_expr | range_search | pp.Group(LPAR + expression + RPAR))
    + pp.Optional(boost)
)
term.setParseAction(lambda t: [t] if "field" in t or "boost" in t else None)

expression << pp.infixNotation(
    term,
    [
        (required_modifier | prohibit_modifier, 1, pp.opAssoc.RIGHT),
        ((not_ | "!").setParseAction(lambda: "NOT"), 1, pp.opAssoc.RIGHT),
        ((and_ | "&&").setParseAction(lambda: "AND"), 2, pp.opAssoc.LEFT),
        (pp.Optional(or_ | "||").setParseAction(lambda: "OR"), 2, pp.opAssoc.LEFT),
    ],
)

if __name__ == "__main__":

    s = "name: \"wal*\" \"hej and\" (hej not haj)"
    out = expression.parseString(s).dump()
    print(out)
    