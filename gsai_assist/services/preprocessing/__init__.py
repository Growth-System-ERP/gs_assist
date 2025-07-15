import re
from rapidfuzz import fuzz
from frappe.utils import now
from gsai_assist.services.preprocessing.stop_words import STOP_WORDS
from gsai_assist.services.preprocessing.entity_mapper import process as map_entity

class TokenList:
    def __init__(self, query):
        self.tokens = []
        self.filtered = []
        self.query = query

        for match in re.finditer(r"\w+", query):
            start, end = match.span()
            text = match.group()

            token = Token(text, start, end)

            self.tokens.append(token)

            if text.lower() not in STOP_WORDS:
                self.filtered.append(token)

    def entatise(self, rwtokens):
        for ogtoken, rwtoken in zip(self.filtered, rwtokens):
            if rwtoken.canonical:
                ogtoken.canonical = rwtoken.canonical

    def rebuild_query(self):
        parts = []
        last_idx = 0

        for token in self.tokens:
            parts.append(self.query[last_idx:token.start])  # Add text between last token and current token
            parts.append(token.canonical or token.text)     # Add canonical replacement or original token
            last_idx = token.end

        parts.append(self.query[last_idx:])  # Add any trailing text
        return "".join(parts)

class Token:
	def __init__(self, text, start, end):
		self.text = text
		self.start = start
		self.end = end
		self.canonical = None

	def entatise(self, entity):
		self.canonical = entity

class PipeLine:
	def process(self, query, opts):
		self.query = query
		self.opts = opts

		self.logs = []

		self.reset()
		self.preprocess()

		return self.rlq, self.context, self.logs

	def reset(self):
		self.rlq = None
		self.context = []
		self.tokens = []

	def preprocess(self):
		self.lq = self.query.lower()

		self.tokenize()

	def tokenize(self):

		self.tokens = TokenList(self.lq)
		self.logs.append(f"mapping entity: {now()}")
		rewritten_tokens, self.context = map_entity(self.tokens.filtered, self.opts.get("entity_groups"), debug=self.opts.get("debug"))

		self.logs.append(f"entitising: {now()}")
		self.tokens.entatise(rewritten_tokens)

		self.rlq = self.tokens.rebuild_query()
