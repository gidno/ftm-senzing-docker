import json
from collections import defaultdict
from typing import List, DefaultDict, Dict
from datetime import datetime
from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry
from followthemoney import helpers
import time

from libs.conf import (RABBIT_Q, RABBIT_Q_ERROR, THING_ENTITY_LIST,
                       INTERVALS_ENTITY_LIST)
from libs.utils import get_q_manager


class MapDict:

    def __init__(self):
        # dicts for interval entities
        self.DIRECTORSHIPS: DefaultDict[str, List[EntityProxy]] = defaultdict(
            list)
        self.EMPLOYMENTS: DefaultDict[str, List[EntityProxy]] = defaultdict(
            list)
        self.MEMBERSHIPS: DefaultDict[str, List[EntityProxy]] = defaultdict(
            list)
        self.REPRESENTATIONS: DefaultDict[
            str, List[EntityProxy]] = defaultdict(list)
        self.UNKNOWN_LINKS: DefaultDict[str, List[EntityProxy]] = defaultdict(
            list)
        self.OWNERSHIPS: DefaultDict[str, List[EntityProxy]] = defaultdict(
            list)
        self.ADDRESSES: Dict[str, EntityProxy] = {}
        self.IDENTIFICATIONS: DefaultDict[
            str, List[EntityProxy]] = defaultdict(list)
        self.FAMILIES: DefaultDict[str, List[EntityProxy]] = defaultdict(list)
        self.ASSOCIATIONS: DefaultDict[str, List[EntityProxy]] = defaultdict(
            list)
        # self.SUCCESIONS: DefaultDict[str, List[EntityProxy]] =
        # defaultdict(list)

    def fill(self, entity):
        if entity.schema.is_a("Succession"):
            return  # will be addeed later
        elif entity.schema.is_a("Directorship"):
            for director in entity.get("director"):
                self.DIRECTORSHIPS[director].append(entity)
        elif entity.schema.is_a("Employment"):
            for employer in entity.get("employer"):
                self.EMPLOYMENTS[employer].append(entity)
        elif entity.schema.is_a("Membership"):
            for member in entity.get("member"):
                self.MEMBERSHIPS[member].append(entity)
        elif entity.schema.is_a("Representation"):
            for agent in entity.get("agent"):
                self.REPRESENTATIONS[agent].append(entity)
        elif entity.schema.is_a("UnknownLink"):
            for subject in entity.get("subject"):
                self.UNKNOWN_LINKS[subject].append(entity)
        elif entity.schema.is_a("Ownership"):
            for owner in entity.get("owner"):
                self.OWNERSHIPS[owner].append(entity)
        elif entity.schema.is_a("Identification"):
            for holder in entity.get("holder"):
                self.IDENTIFICATIONS[holder].append(entity)
        elif entity.schema.is_a("Address"):
            self.ADDRESSES[entity.id] = entity
        elif entity.schema.is_a("Family"):
            for person in entity.get("person"):
                self.FAMILIES[person].append(entity)
        elif entity.schema.is_a("Associate"):
            for person in entity.get("person"):
                self.ASSOCIATIONS[person].append(entity)


class SenzingMapper:

    def __init__(self, source_file, logger, datasource):

        self.datasource = datasource
        self.source_file = source_file
        self.logger = logger
        self.md = MapDict()

    # to get unknown entities
    def catch_unk_entities(self):

        start_time = datetime.now()
        other = []
        known_types_list = THING_ENTITY_LIST + INTERVALS_ENTITY_LIST
        with open(self.source_file, "r") as fh:
            while line := fh.readline():
                data = json.loads(line)
                entity = model.get_proxy(data)
                if entity.schema.name not in known_types_list:
                    other.append(entity)
        self.logger.info(f'Found {len(other)} unknown entities in source '
                         f'file, time spent: {datetime.now() - start_time}')

    # for reading known entities
    def read_entities(self):
        # Interval entities first - stored in interval Dicts
        self.logger.info(f"Caching aux entities: {self.source_file}", )
        for line in self.source_file:
            line = line.strip()
            data = json.loads(line)
            entity = model.get_proxy(data)
            if entity.schema.name in INTERVALS_ENTITY_LIST:
                self.md.fill(entity)
            if entity.schema.name in THING_ENTITY_LIST:
                yield entity

    # get only one attribute
    @staticmethod
    def get_attribute(entity, prop, attr):
        value = entity.get(prop, quiet=True)
        if value:
            return {attr: value[0]}
        else:  # empty list
            return {attr: value}

    # get only attributes_list
    @staticmethod
    def get_attribute_list(entity, props, attr):
        values = []
        for prop in props:
            values += entity.get(prop, quiet=True)
        return {attr + '_LIST': [{attr: value} for value in values]}

    # for disclosed relationships
    def create_disclosed_relationships(
            self, relationship_list, intervals_dict, entity_id, role_name,
            subj_name, anchor):
        for adj in intervals_dict.get(entity_id, []):
            for role in adj.get(role_name, quiet=True):
                if not anchor:
                    relationship_list += [{
                        "REL_ANCHOR_DOMAIN": self.datasource,
                        "REL_ANCHOR_KEY": entity_id}]
                    anchor = True
                for subject in set(adj.get(subj_name, quiet=True)):
                    relationship_list += [{
                        "REL_POINTER_DOMAIN": self.datasource,
                        "REL_POINTER_KEY": subject,
                        "REL_POINTER_ROLE": role
                    }]
        return relationship_list, anchor

    @staticmethod
    def __map_names(entity, name_field):
        name_list = []
        # for a while not using firstName, secondName, middleName,
        # lastName, fatherName, motherName
        for name in entity.get_type_values(registry.name):
            name_type = "PRIMARY" if name == entity.caption else "ALIAS"
            if not isinstance(name, list):
                name_list.append({"NAME_TYPE": name_type, name_field: name})
            else:
                for name_el in name:
                    name_list.append(
                        {"NAME_TYPE": name_type, name_field: name_el})

        if entity.has('weakAlias'):
            weak_alias_list = [y for x in entity.get('weakAlias', quiet=True)
                               for y in x.split('\n') if y]
            for other_alias in weak_alias_list:
                name_list.append(
                    {"NAME_TYPE": "ALIAS", name_field: other_alias})
        return name_list

    def __map_addresses(self, entity, addr_type):
        addr_list = []
        for addr_id in entity.get("addressEntity"):
            addr = self.md.ADDRESSES.get(addr_id)
            if not addr:
                continue
            elif addr.has("postalCode") or addr.has("city"):
                addr_data = {
                    "ADDR_TYPE": addr_type,
                    "ADDR_LINE1": addr.first("street"),
                    "ADDR_LINE2": addr.first("street2"),
                    "ADDR_CITY": addr.first("city"),
                    "ADDR_STATE": addr.first("state"),
                    "ADDR_COUNTRY": addr.first("country"),
                    "ADDR_POSTAL_CODE": addr.first("postalCode")
                }
            else:
                addr_data = {
                    "ADDR_TYPE": addr_type,
                    "ADDR_FULL": addr.first("full")
                }
            addr_type = "OTHER"
            addr_list.append(addr_data)

        for value in entity.get('address', quiet=True):
            addr_data = {
                "ADDR_TYPE": addr_type,
                "ADDR_FULL": value
            }
            addr_type = "OTHER"  # if it is correct type for address?
            addr_list.append(addr_data)
        return addr_list

    def __map_relationships(self, entity):
        relations = ((self.md.UNKNOWN_LINKS, 'role', 'object'),
                     (self.md.OWNERSHIPS, 'role', 'asset'),
                     (self.md.DIRECTORSHIPS, 'role', 'organization'),
                     (self.md.EMPLOYMENTS, 'role', 'eployee'),
                     (self.md.MEMBERSHIPS, 'role', 'organiztion'),
                     (self.md.REPRESENTATIONS, 'role', 'client'),
                     (self.md.FAMILIES, 'relationship', 'relative'),
                     (self.md.ASSOCIATIONS, 'relationship', 'associate')
                     )
        relationship_list = []
        anchor = False
        for _dict, role, subj in relations:
            relationship_list, anchor = self.create_disclosed_relationships(
                relationship_list, _dict, entity.id, role, subj, anchor)
        return relationship_list

    # map_record function
    def transform(self, entity: EntityProxy, alias_split=True):
        entity = helpers.simplify_provenance(entity)
        record = {
            "DATA_SOURCE": self.datasource,
            "RECORD_ID": entity.id,
        }
        # DEFINE RECORD TYPE
        is_org = False
        if entity.schema.name == "Person":
            record["RECORD_TYPE"] = "PERSON"
            addr_type = "HOME"
            name_field = "NAME_FULL"
            attr = self.get_attribute_list(entity, ["country"], "CITIZENSHIP")
            if attr["CITIZENSHIP_LIST"]:
                if len(attr["CITIZENSHIP_LIST"]) > 1:
                    record.update(attr)
                else:
                    record.update(attr["CITIZENSHIP_LIST"][0])
        elif entity.schema.name in ("Organization", "Company", "PublicBody"):
            record["RECORD_TYPE"] = "ORGANIZATION"
            addr_type = "BUSINESS"
            name_field = "NAME_ORG"
            attr = self.get_attribute_list(entity, ["mainCountry"],
                                           "REGISTRATION_COUNTRY")
            if attr["REGISTRATION_COUNTRY_LIST"]:
                if len(attr["REGISTRATION_COUNTRY_LIST"]) > 1:
                    record.update(attr)
                else:
                    record.update(attr["REGISTRATION_COUNTRY_LIST"][0])
            is_org = True
        else:
            addr_type = "LEGAL"
            name_field = "NAME_FULL"

        # NAME LIST
        record['NAME_LIST'] = self.__map_names(entity, name_field)

        # ADDRESS LIST
        addr_list = self.__map_addresses(entity, addr_type)
        if len(addr_list):
            record["ADDRESS_LIST"] = addr_list

        # PERSONAL DATA
        for gender in entity.get("gender", quiet=True):
            record["GENDER"] = "M" if gender == "male" else "F"
        map_dict_single = {
            "DATE_OF_BIRTH": "birthDate",
            "DATE_OF_DEATH": "deathDate",
            "PLACE_OF_BIRTH": "birthPlace",
            "NATIONALITY": "nationality",
            "REGISTRATION_DATE": "incorporationDate",
            "NATIONAL_ID_NUMBER": "idNumber",
            "TAX_ID_NUMBER": "taxNumber"
        }
        for key, value in map_dict_single.items():
            attr = self.get_attribute(entity, value, key)
            if attr.get(key):
                record.update(attr)

        map_dict_lists = {
            "WEBSITE_ADDRESS": ["website"],
            "EMAIL_ADDRESS": ["email"],
            "PHONE_NUMBER": ["phone"],
            "COUNTRY_OF_ASSOCIATION": ["jurisdiction"]
        }
        if is_org:
            map_dict_lists["COUNTRY_OF_ASSOCIATION"].append('country')
        for key, value in map_dict_lists.items():
            attr = self.get_attribute_list(entity, value, key)
            if attr.get(key + '_LIST'):
                if len(attr[key + "_LIST"]) > 1:
                    record.update(attr)
                else:
                    record.update(attr[key + "_LIST"][0])

        # RELATIONSHIP
        relationship_list = self.__map_relationships(entity)
        if relationship_list:
            record.update({"RELATIONSHIP_LIST": relationship_list})

        # DOCS
        for adj in self.md.IDENTIFICATIONS.get(entity.id, []):
            if adj.schema.is_a("Passport"):
                record.update({
                    "PASSPORT_NUMBER": adj.first("number"),
                    "PASSPORT_COUNTRY": adj.first("country"),
                })
        if 'PASSPORT_NUMBER' not in record.keys():
            attr = self.get_attribute(
                entity, "passportNumber", "PASSPORT_NUMBER")
            if attr.get("PASSPORT_NUMBER"):
                record.update(attr)

        map_dict_list = {
            "INN_CODE": "innCode",
            "VAT_CODE": "vatCode",
            "DUNS_NUMBER": "dunsCode",
            "SWIFT_BIC_CODE": "swiftBic",
            "ICIJ_ID_CODE": "icijId",
            "OKPO_CODE": "okpoCode",
            "BVDID_CODE": "bvdid"
        }
        if is_org:
            map_dict_list.update({
                "VOEN_CODE": "voenCode",
                "BIK_CODE": "bikCode",
                "IRS_CODE": "irsCode",
                "IPO_CODE": "ipoCode",
                "CIK_CODE": "cikCode",
                "JIB_CODE": "jibCode",
                "CAEM_CODE": "caemCode",
                "COATO_CODE": "coatoCode",
                # "MBS_CODE":        "mbsCode",
                # "IBC_RUC_CODE":    "ibcRuc",
                "OGRN_CODE": "ogrnCode",
                "PRF_NUMBER_CODE": "pfrNumber",
                "OKSM_CODE": "oksmCode"
            })
        for key, value in map_dict_list.items():
            attr = self.get_attribute(entity, value, key)
            if attr.get(key):
                record.update(attr)
        return record

    def run(self, alias_split=True, catch_unknown_entities=False):

        try:
            start_time = datetime.now()
            q_manager = get_q_manager(RABBIT_Q, RABBIT_Q_ERROR)

            self.logger.info(f'Processing file: {self.source_file}')
            self.logger.info(f'Data Source: {self.datasource}')

            if catch_unknown_entities:
                self.catch_unk_entities()
            total = 0
            for entity in self.read_entities():
                record = self.transform(entity, alias_split)
                i = 5
                err = None
                while i > 0:
                    try:
                        q_manager.basic_publish(
                            exchange='',
                            routing_key=RABBIT_Q,
                            body=json.dumps(record))
                        total += 1
                        break
                    except Exception as e:
                        err = e
                        time.sleep(i * 5)
                        i -= 1
                else:
                    self.logger.error(f'Error send message to Rabbit: {err}')
                    q_manager.basic_publish(
                        exchange='',
                        routing_key=RABBIT_Q_ERROR,
                        body={'task': json.dumps(record), 'error': err})

            self.logger.info(f'{total} records processed! Time spent: '
                             f'{datetime.now() - start_time}')
            return {'Status': 'OK'}
        except Exception as e:
            self.logger.exception(f'ERROR: {e}')
            raise e
