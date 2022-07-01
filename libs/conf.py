import os

# lists of entity types used in mapper
# "Asset" - not used for now
THING_ENTITY_LIST = ["Person", "Organization", "Company",
                     "LegalEntity",
                     "PublicBody"]
# "ProjectParticipant", "ContractAward",
# "Documentation", "CourtCaseParty" - not used for now
INTEREST_ENTITY_LIST = ["Succession", "Directorship", "Employment",
                        "Membership", "Representation",
                        "UnknownLink",
                        "Ownership"]

INTERVALS_ENTITY_LIST = INTEREST_ENTITY_LIST + ["Identification",
                                                "Address",
                                                "Family",
                                                "Associate"]
VERBOSE_LOG = False

RABBIT_URL = 'rabbitmq3'
RABBIT_USER = 'senzing'
RABBIT_PASS = 'senzing'
RABBIT_Q = 'load_senzing_q'
RABBIT_Q_ERROR = 'load_senzing_q_err'
SLACK_WEBHOOK = "https://hooks.slack.com/services/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"

INIT_JSON = os.getenv('INIT_JSON')
CONFIG_JSON = os.getenv('CONFIG_JSON')
FORCE_LOAD_CONFIG = False
