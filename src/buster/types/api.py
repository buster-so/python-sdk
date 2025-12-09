from enum import Enum


class ApiVersion(str, Enum):
    V2 = "v2"


class Environment(str, Enum):
    PRODUCTION = "production"
    DEVELOPMENT = "development"
    STAGING = "staging"
