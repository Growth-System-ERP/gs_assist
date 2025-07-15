import spacy
from symspellpy import SymSpell, Verbosity
import pkg_resources

nlp = spacy.load("en_core_web_lg")
sym_spell = SymSpell(max_dictionary_edit_distance=2)

# Load dictionary
dictionary_path = pkg_resources.resource_filename("symspellpy", "frequency_dictionary_en_82_765.txt")
sym_spell.load_dictionary(dictionary_path, term_index=0, count_index=1)

def process(text):
    doc = nlp(text)
    corrected = []

    for token in doc:
        if token.is_alpha and len(token.text) > 3:
            suggestions = sym_spell.lookup(token.text, Verbosity.CLOSEST, max_edit_distance=2)
            if suggestions:
                corrected.append(suggestions[0].term)
            else:
                corrected.append(token.text)
        else:
            corrected.append(token.text)

    return " ".join(corrected)