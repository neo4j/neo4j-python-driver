import neo4j

from ._warning_check import warning_check


with warning_check(neo4j.PreviewWarning, "GQLSTATUS"):
    from neo4j import NotificationDisabledClassification


__all__ = [
    "NotificationDisabledClassification",
]
