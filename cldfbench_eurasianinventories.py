"""
Generates a CLDF dataset for Nikolaev's "The database of Eurasian phonological
inventories" (2020).
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

    text = unicodedata.normalize("NFC", text)

    if text[0] == "(" and text[-1] == ")":
        text = text[1:-1]

    if text[0] == "[" and text[-1] == "]":
        text = text[1:-1]

    return text


class Dataset(BaseDataset):
    """
    CLDF dataset for inventories.
    """

    dir = Path(__file__).parent
    id = "eurasianinventories"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(dir=self.cldf_dir, module="StructureDataset")

    def cmd_download(self, args):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw_dir`, e.g.

        >>> self.raw_dir.download(url, fname)
        """
        pass

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """

        # Instantiate Glottolog and CLTS
        # TODO: how to call CLTS?
        glottolog = Glottolog(args.glottolog.dir)
        clts_path = Path.home() / ".config" / "cldf" / "clts"
        clts = CLTS(clts_path)

        # Add components
        args.writer.cldf.add_columns(
            "ValueTable",
            {"name": "Marginal", "datatype": "boolean"},
            {"name": "Allophones", "separator": " "},
            "Contribution_ID",
        )

        args.writer.cldf.add_component("ParameterTable", "BIPA")
        args.writer.cldf.add_component(
            "LanguageTable", "Family_Glottocode", "Family_Name", "Glottolog_Name"
        )
        args.writer.cldf.add_table(
            "inventories.csv",
            "ID",
            "Name",
            "Contributor_ID",
            {
                "name": "Source",
                "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#source",
                "separator": ";",
            },
            "URL",
            "Tones",
            primaryKey="ID",
        )

        # load language mapping and build inventory info
        languages = []
        inventories = []
        lang_map = {}
        for row in self.etc_dir.read_csv("languages.csv", dicts=True):
            # Add mapping, if any
            lang_map[row["name"]] = slug(row["name"])
            lang_dict = {"ID": slug(row["name"]), "Name": row["name"]}
            if row["glottocode"]:
                lang = glottolog.languoid(row["glottocode"])
                lang_dict.update(
                    {
                        "Family_Glottocode": lang.lineage[0][1] if lang.lineage else None,
                        "Family_Name": lang.lineage[0][0] if lang.lineage else None,
                        "Glottocode": lang.id,
                        "ISO639P3code": lang.iso_code,
                        "Latitude": lang.latitude,
                        "Longitude": lang.longitude,
                        "Macroarea": lang.macroareas[0].name if lang.macroareas else None,
                        "Glottolog_Name": lang.name,
                    }
                )
            languages.append(lang_dict)

            # collect inventory info
            inventories.append(
                {
                    "Contributor_ID": None,
                    "ID": slug(row["name"]),
                    "Name": row["name"],
                    "Source": [],
                    "URL": None,
                    "Tones": None,
                }
            )

        # Read raw data
        with open("raw/phono_dbase.json") as handler:
            raw_data = json.load(handler)

        # Iterate over raw data
        values = []
        parameters = []
        inventories = []
        counter = 1
        segment_set = set()
        for idx, (language, langdata) in enumerate(raw_data.items()):
            cons = langdata["cons"]
            vows = langdata["vows"]
            tones = [tone for tone in langdata["tones"] if tone]

            # Collect inventory info
            inventories.append(
                {
                    "Contributor_ID": None,
                    "ID": idx + 1,
                    "Name": language,
                    "Source": [],
                    "URL": None,
                    "Tones": ",".join(tones),
                }
            )

            # Add consonants and vowels to values, also collecting parameters
            for segment in cons + vows:
                marginal = bool(segment[0] == "(")

                # Obtain the corresponding BIPA grapheme, is possible
                normalized = normalize_grapheme(segment)
                sound = clts.bipa[normalized]
                if isinstance(sound, models.UnknownSound):
                    par_id = "UNK_" + compute_id(normalized)
                    bipa_grapheme = ""
                    desc = ""
                else:
                    par_id = "BIPA_" + compute_id(normalized)
                    bipa_grapheme = str(sound)
                    desc = sound.name
                parameters.append((par_id, normalized, bipa_grapheme, desc))

                # Prepare language key
                lang_key = language.split("#")[0].replace(",", "")

                values.append(
                    {
                        "ID": str(counter),
                        "Language_ID": lang_map[lang_key],
                        "Marginal": marginal,
                        "Parameter_ID": par_id,
                        "Value": segment,
                        "Source": [],  # TODO: add Nikolaev as source
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
                "inventories.csv": inventories,
            }
        )
