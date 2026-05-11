"""
Copyright 2021 Gabriele Pisciotta - ga.pisciotta@gmail.com

Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted,
provided that the above copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE
OF THIS SOFTWARE.
"""

__author__ = "Gabriele Pisciotta"

import contextlib
import datetime
import sys
from typing import Union

import requests_cache

# MODIFICA:
# Rimosso WikiData dagli import.

from oc_graphenricher.APIs import Crossref, ORCID, VIAF, OpenAlex

from oc_ocdm import Storer
from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.entities.bibliographic.bibliographic_resource import BibliographicResource
from oc_ocdm.graph.entities.bibliographic.responsible_agent import ResponsibleAgent
from oc_ocdm.graph.graph_entity import GraphEntity
from oc_ocdm.prov import ProvSet
from tqdm import tqdm
from tqdm.contrib import DummyTqdmFile


class GraphEnricher:
    """
    The GraphEnricher class is the one responsible to enrich all the entities in a given graph set compliant to
    the OpenCitations Data Model (OCDM). You have to specify in input the graph set, the output file name of the
    enriched graph and the provenance file name. It's also possible to specify a debug flag to get more details
    about the enrichment process.
    """

    def __init__(self,
                 g_set: GraphSet,
                 graph_filename: str = "enriched.rdf",
                 provenance_filename: str = "provenance.rdf",
                 info_dir: str = "",
                 debug: bool = False,
                 serialize_in_the_middle: bool = False):
        """

        :param g_set: graph set to be enriched
        :param graph_filename: file name of the enriched graph set that will be serialized
        :param provenance_filename: file name of the provenance that will be serialized
        :param info_dir: the path to the counters directory
        :param debug: a bool flag to enable richer output
        :param serialize_in_the_middle: a bool flag to enable the serialization each 50 Bibliographic Resources (BRs)
        processed (the resulting file will be always overwritten, this may slow the whole process)
        """

        requests_cache.install_cache('GraphEnricher_cache')

        self.resp_agent = 'https://w3id.org/oc/meta/prov/pa/2'
        self.crossref_api = Crossref()
        self.orcid_api = ORCID()
        self.viaf_api = VIAF()

        # MODIFICA:
        # Rimossa inizializzazione API Wikidata.

        self.openalex_api = OpenAlex()
        self.g_set = g_set
        self.debug = debug
        self.new_id_found = 0
        self.graph_filename = graph_filename
        self.provenance_filename = provenance_filename
        self.info_dir = info_dir
        self.serialize_in_the_middle = serialize_in_the_middle

    def enrich(self) -> None:
        """
        MODIFICA:
        Aggiornata la documentazione della funzione.

        Rimossi:
        - riferimenti a Wikidata ID
        - query a Wikidata per BR e AR

        Ora:
        - controlla DOI, ISSN e OpenAlex ID
        - arricchisce con OpenAlex ma non più con Wikidata
        """

        br_enriched_counter = 0

        with self.__std_out_err_redirect_tqdm() as orig_stdout:

            # MODIFICA:
            # Aggiunti position=0 e leave=True alla progress bar.
            progress_bar = tqdm(
                self.g_set.get_br(),
                file=orig_stdout,
                dynamic_ncols=True,
                position=0,
                leave=True
            )

            for br in progress_bar:
                progress_bar.set_description(desc=f"New ID found: {self.new_id_found}")

                br_enriched_counter += 1

                if br_enriched_counter % 50 == 0 and self.serialize_in_the_middle:
                    gs_storer = Storer(self.g_set, output_format="nt11")
                    gs_storer.store_graphs_in_file(self.graph_filename, "")

                if (
                    GraphEntity.iri_journal_issue in br.get_types()
                    or GraphEntity.iri_journal_volume in br.get_types()
                ):
                    continue

                authors = []
                publisher_has_crossrefid = False

                # Extract br's identifiers
                has_doi = None
                has_issn = []
                has_openalex = None

                # MODIFICA:
                # Rimossa gestione has_wikidata.
                for i in br.get_identifiers():

                    if i.get_scheme() == br.iri_doi:
                        has_doi = i.get_literal_value()

                    elif i.get_scheme() == br.iri_issn:
                        has_issn.append(i.get_literal_value())

                    elif i.get_scheme() == br.iri_openalex:
                        has_openalex = i.get_literal_value()

                # Get more ISSNs
                if len(has_issn) > 0:

                    for issn in has_issn:
                        res = self.crossref_api.query_journal(issn)

                        if res:
                            for r in res:

                                # To avoid to add already present ISSNs
                                if r not in has_issn:
                                    self._add_id(
                                        br,
                                        r,
                                        'issn',
                                        "its ISSN {}".format(issn)
                                    )
                            break

                # MODIFICA:
                # Aggiunto controllo sul titolo prima della query Crossref.
                # Evita query inutili se titolo mancante o "unknown".
                _title = br.get_title()

                if (
                    has_doi is None
                    and _title
                    and _title.strip().lower() != "unknown"
                ):

                    res = self.crossref_api.query(
                        authors,
                        _title,
                        br.get_pub_date()
                    )

                    if res:
                        self._add_id(br, res, 'doi', "Crossref query")
                        has_doi = res

                # MODIFICA:
                # Rimossa completamente la sezione:
                # "If it hasn't a Wikidata ID..."
                # che interrogava Wikidata tramite DOI/ISSN/PMID/PMCID.

                # If it has no OpenAlex ID, extract identifiers and search on OpenAlex
                if has_openalex is None:

                    for i in br.get_identifiers():

                        if i.get_scheme() == br.iri_issn:
                            res: list = self.openalex_api.query(
                                i.get_literal_value(),
                                'issn'
                            )

                            if res:
                                for oaid in res:
                                    self._add_id(
                                        br,
                                        oaid,
                                        'openalex',
                                        f"its ISSN {i.get_literal_value()}"
                                    )

                        if i.get_scheme() == br.iri_doi:
                            res = self.openalex_api.query(
                                i.get_literal_value(),
                                'doi'
                            )

                            if res:
                                for oaid in res:
                                    self._add_id(
                                        br,
                                        oaid,
                                        'openalex',
                                        f"its DOI {i.get_literal_value()}"
                                    )

                        if i.get_scheme() == br.iri_pmid:
                            res = self.openalex_api.query(
                                i.get_literal_value(),
                                'pmid'
                            )

                            if res:
                                for oaid in res:
                                    self._add_id(
                                        br,
                                        oaid,
                                        'openalex',
                                        f"its PMID {i.get_literal_value()}"
                                    )

                        if i.get_scheme() == br.iri_pmcid:
                            res = self.openalex_api.query(
                                i.get_literal_value(),
                                'pmcid'
                            )

                            if res:
                                for oaid in res:
                                    self._add_id(
                                        br,
                                        oaid,
                                        'openalex',
                                        f"its PMCID {i.get_literal_value()}"
                                    )

                for ar in br.get_contributors():

                    role = ar.get_role_type()
                    ra: ResponsibleAgent = ar.get_is_held_by()

                    # Extract Authors
                    if role == GraphEntity.iri_author:

                        authors.append((
                            ra.get_given_name(),
                            ra.get_family_name(),
                            ra
                        ))

                        has_orcid = None
                        has_viaf = None

                        # MODIFICA:
                        # Rimossa variabile has_wikidata.
                        author_id_found = []

                        for author_identifier in ra.get_identifiers():

                            if ra.iri_orcid in author_identifier.get_scheme():
                                has_orcid = author_identifier.get_literal_value()
                                author_id_found.append((
                                    author_identifier.get_literal_value(),
                                    'orcid'
                                ))

                            if br.iri_viaf in author_identifier.get_scheme():
                                has_viaf = author_identifier.get_literal_value()
                                author_id_found.append((
                                    author_identifier.get_literal_value(),
                                    'viaf'
                                ))

                            # MODIFICA:
                            # Rimossa gestione Wikidata autore.

                            # MODIFICA:
                            # Rimossa condizione su has_wikidata.
                            if (
                                has_viaf is not None
                                and has_orcid is not None
                            ):
                                break

                        if has_orcid is None:

                            res = self.orcid_api.query(
                                [(
                                    ra.get_given_name(),
                                    ra.get_family_name(),
                                    None,
                                    ra
                                )],
                                [
                                    (
                                        x.get_scheme(),
                                        x.get_literal_value()
                                    )
                                    for x in br.get_identifiers()
                                ]
                            )

                            if res:
                                for res_tuple in res:

                                    given_name, family_name, orcid, ra = res_tuple

                                    if orcid is not None:
                                        self._add_id(ra, orcid, 'orcid')
                                        author_id_found.append((
                                            orcid,
                                            'orcid'
                                        ))

                        if not has_viaf:

                            viaf = self.viaf_api.query(
                                ra.get_given_name(),
                                ra.get_family_name(),
                                br.get_title()
                            )

                            if viaf is not None:
                                self._add_id(ra, viaf, 'viaf')

                        # MODIFICA:
                        # Rimossa completamente la sezione:
                        # "If the author doesn't have Wikidata ID"

                    # Get Publisher and its identifiers
                    if role == GraphEntity.iri_publisher:

                        for publisher_id in ra.get_identifiers():

                            if GraphEntity.iri_crossref in publisher_id.get_scheme():
                                publisher_has_crossrefid = True
                                break

                        # If crossref-id not found, search it
                        if (
                            not publisher_has_crossrefid
                            and has_doi is not None
                        ):

                            crossref_id = self.crossref_api.query_publisher(has_doi)

                            if crossref_id:
                                self._add_id(ra, crossref_id, 'crossref')

            gs_storer = Storer(self.g_set, output_format="nt11")
            gs_storer.store_graphs_in_file(self.graph_filename, "")

            prov = ProvSet(
                self.g_set,
                self.g_set.base_iri,
                info_dir=self.info_dir
            )

            prov.generate_provenance()

            prov_storer = Storer(prov, output_format="nquads")
            prov_storer.store_graphs_in_file(self.provenance_filename, "")

    def _add_id(
        self,
        entity: Union[BibliographicResource, ResponsibleAgent],
        literal: str,
        schema: str,
        by_means_of: str = None
    ) -> None:

        """
        Method that let you add a new identifier to an entity
        """

        old_identifiers = entity.get_identifiers()

        # Check if the ID is already associated to the entity
        for i in old_identifiers:

            if i.get_literal_value() == literal:

                if self.debug:
                    print("Identifier {} already present".format(literal))

                return

        self.new_id_found += 1

        new_id = self.g_set.add_id(self.resp_agent)

        if schema == 'issn':
            new_id.create_issn(literal)

        elif schema == 'doi':
            new_id.create_doi(literal)

        elif schema == 'orcid':
            new_id.create_orcid(literal)

        elif schema == 'viaf':
            new_id.create_viaf(literal)

        elif schema == 'crossref':
            new_id.create_crossref(literal)

        # MODIFICA:
        # Rimossa gestione schema == 'wikidata'

        elif schema == 'openalex':
            new_id.create_openalex(literal)

        entity.has_identifier(new_id)

        if self.debug:

            to_print = "[{}] FOUND {}: {}".format(
                datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
                schema,
                literal
            )

            if by_means_of is not None:
                to_print += ", by means of {}".format(by_means_of)

            print(to_print)
            print("\tOLD: {}".format(
                [x.get_literal_value() for x in old_identifiers]
            ))

            print("\tNEW : {}".format(
                [x.get_literal_value() for x in entity.get_identifiers()]
            ))

    @contextlib.contextmanager
    def __std_out_err_redirect_tqdm(self):

        """ This method is used to print messages with the TQDM progress bar"""

        orig_out_err = sys.stdout, sys.stderr

        try:
            sys.stdout, sys.stderr = map(DummyTqdmFile, orig_out_err)
            yield orig_out_err[0]

        except Exception as exc:
            raise exc

        finally:
            sys.stdout, sys.stderr = orig_out_err
