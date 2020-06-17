import json
import pathlib
from unidecode import unidecode

from cldfbench import CLDFSpec
from cldfbench import Dataset as BaseDataset
from clldutils.misc import slug

from pyglottolog import Glottolog


def compute_id(text):
    """
    Returns a codepoint representation to an Unicode string.
    """

    unicode_repr = ["U{0:0{1}X}".format(ord(char), 4) for char in text]

    return slug("%s_%s" % ("_".join(unicode_repr), unidecode(text)))


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
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

        # Instantiate glottolog
        g = Glottolog(args.glottolog.dir)

        # Add components
        args.writer.cldf.add_columns(
            "ValueTable",
            {"name": "Marginal", "datatype": "boolean"},
            {"name": "Allophones", "separator": " "},
            "Contribution_ID",
        )

        args.writer.cldf.add_component("ParameterTable")
        args.writer.cldf.add_component(
            "LanguageTable", "Family_Glottocode", "Family_Name"
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
        glottocodes = {}
        for row in self.etc_dir.read_csv("languages.csv", dicts=True):
            # extend language info
            languages.append(row)

            # Add mapping, if any
            if row["glottocode"]:
                glottocodes[row["name"]] = row["glottocode"]

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

        # Map glottolog from etc/languages.csv
        languoids = {
            lang.id: {
                "Family_Glottocode": lang.lineage[0][1] if lang.lineage else None,
                "Family_Name": lang.lineage[0][0] if lang.lineage else None,
                "Glottocode": lang.id,
                "ID": lang.id,
                "ISO639P3code": lang.iso_code,
                "Latitude": lang.latitude,
                "Longitude": lang.longitude,
                "Macroarea": lang.macroareas[0].name if lang.macroareas else None,
                "Name": lang.name,
            }
            for lang in g.languoids()
            if lang.id in glottocodes.values()
        }

        # Read raw data
        with open("raw/phono_dbase.json") as handler:
            raw_data = json.load(handler)

        # Iterate over raw data
        values = []
        inventories = []
        counter = 1
        segment_set = set()
        for idx, (language, langdata) in enumerate(raw_data.items()):
            if language.split("#")[0] not in glottocodes:
                continue

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

            # Add consonants and vowels to values
            for segment in cons + vows:
                if segment[0] == "(":
                    segment = segment[1:-1]
                    marginal = True
                else:
                    marginal = False

                values.append(
                    {
                        "ID": str(counter),
                        "Language_ID": glottocodes[language.split("#")[0]],
                        "Marginal": marginal,
                        "Parameter_ID": compute_id(segment),  # Compute
                        "Value": segment,
                        "Source": [],  # TODO
                    }
                )
                counter += 1

                # collect segment
                segment_set.add(segment)

        # Build segment data
        segments = [
            {"Name": segment, "ID": compute_id(segment)} for segment in segment_set
        ]

        # Write data and validate
        args.writer.write(
            **{
                "ValueTable": values,
                "LanguageTable": languoids.values(),
                "ParameterTable": segments,
                "inventories.csv": inventories,
            }
        )
