"""
Generates a CLDF dataset for Nikolaev's "The database of Eurasian phonological inventories" (2020).
"""

import json
import unicodedata
from pathlib import Path
from unidecode import unidecode

from pyglottolog import Glottolog
from pyclts import CLTS, models

from cldfbench import CLDFSpec
from cldfbench import Dataset as BaseDataset
from clldutils.misc import slug
from cldfcatalog.config import Config

from tqdm import tqdm as progressbar
from collections import defaultdict



def compute_id(text):
    """
    Returns a codepoint representation to an Unicode string.
    """

    unicode_repr = "".join(["u{0:0{1}X}".format(ord(char), 4) for char in text])

    label = slug(unidecode(text))

    return "%s_%s" % (label, unicode_repr)


def normalize_grapheme(text):
    """
    Apply simple, non-CLTS, normalization.
    """

def normalize_grapheme(text):
    """
    Apply simple, non-CLTS, normalization.
    """

    new_text = unicodedata.normalize("NFD", text)
    new_text = new_text.replace('\u2019', '\u02bc')
    return new_text


class Dataset(BaseDataset):
    """
    CLDF dataset for inventories.
    """

    dir = Path(__file__).parent
    id = "eurasianinventories"

    def cldf_specs(self):
        return CLDFSpec(
                module='StructureDataset',
                dir=self.cldf_dir,
                data_fnames={'ParameterTable': 'features.csv'}
            )

    def cmd_makecldf(self, args):

        glottolog = Glottolog(args.glottolog.dir)
        clts = CLTS(Config.from_file().get_clone('clts'))
        bipa = clts.bipa
        clts_eurasian = clts.transcriptiondata_dict['eurasian']

        args.writer.cldf.add_columns(
            "ValueTable",
            {"name": "Marginal", "datatype": "boolean"},
            {"name": "Value_in_Source", "datatype": "string"})

        args.writer.cldf.add_columns(
                    'ParameterTable',
                    {'name': 'CLTS_BIPA', 'datatype': 'string'},
                    {'name': 'CLTS_Name', 'datatype': 'string'})
        args.writer.cldf.add_component(
            "LanguageTable", "Family", "Glottolog_Name"
        )

        # load language mapping and build inventory info
        languages = []
        lang_map = {}
        all_glottolog = {lng.id: lng for lng in glottolog.languoids()}
        unknowns = defaultdict(list)
        for row in progressbar(
                self.etc_dir.read_csv( "languages.csv", dicts=True)):
            lang_map[row["name"]] = slug(row["name"])
            lang_dict = {"ID": slug(row["name"]), "Name": row["name"]}
            if row["glottocode"] in all_glottolog:
                lang = all_glottolog[row["glottocode"]]
                lang_dict.update(
                    {
                        "Family": lang.family if lang.lineage else None,
                        "Glottocode": lang.id,
                        "ISO639P3code": lang.iso_code,
                        "Latitude": lang.latitude,
                        "Longitude": lang.longitude,
                        "Macroarea": lang.macroareas[0].name if lang.macroareas else None,
                        "Glottolog_Name": lang.name,
                    }
                )
            languages.append(lang_dict)


        # Read raw data
        with open(self.raw_dir.joinpath('phono_dbase.json').as_posix()) as handler:
            raw_data = json.load(handler)

        # Iterate over raw data
        values = []
        parameters = []
        inventories = []
        counter = 1
        segment_set = set()
        with open(self.raw_dir.joinpath('sources.txt').as_posix()) as f:
            sources = [source.strip() for source in f.readlines()][1:]
        args.writer.cldf.add_sources()
        for idx, (language, langdata) in enumerate(raw_data.items()):
            cons = langdata["cons"]
            vows = langdata["vows"]
            tones = [tone for tone in langdata["tones"] if tone]
            source = sources[idx]
            # Prepare language key
            lang_key = language.split("#")[0].replace(",", "")

            # Add consonants and vowels to values, also collecting parameters
            for segment in cons + vows:
                marginal = bool(segment[0] == "(")




                # Obtain the corresponding BIPA grapheme, is possible
                normalized = normalize_grapheme(segment)
                par_id = compute_id(normalized)
                if normalized in clts_eurasian.grapheme_map:
                    sound = bipa[clts_eurasian.grapheme_map[normalized]]
                else:
                    sound = bipa['<NA>']
                    unknowns[normalized] += [(segment, lang_key)]
                if sound.type == 'unknownsound':
                    bipa_grapheme = ''
                    desc = ''
                else:
                    bipa_grapheme = str(sound)
                    desc = sound.name
                parameters.append((par_id, normalized, bipa_grapheme, desc))


                values.append(
                    {
                        "ID": str(counter),
                        "Language_ID": lang_map[lang_key],
                        "Marginal": marginal,
                        "Parameter_ID": par_id,
                        "Value": normalized,
                        "Value_in_Source": segment,
                        "Source": [source],
                    }
                )
                counter += 1

        # Build segment data
        segments = [
            {"ID": id, "Name": normalized, "BIPA": bipa_grapheme, "Description": desc}
            for id, normalized, bipa_grapheme, desc in set(parameters)
        ]

        # Write data and validate
        args.writer.write(
            **{
                "ValueTable": values,
                "LanguageTable": languages,
                "ParameterTable": segments,
            }
        )
        for g, rest in unknowns.items():
            print('\t'.join(
                [
                    repr(g), str(len(rest)), g]))
