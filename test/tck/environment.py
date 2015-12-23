import logging

from test.tck import tck_util
from behave.log_capture import capture


def before_all(context):
    # -- SET LOG LEVEL: behave --logging-level=ERROR ...
    # on behave command-line or in "behave.ini".
    context.config.setup_logging()


@capture
def after_scenario(context, scenario):
    for step in scenario.steps:
        if step.status == 'failed':
            logging.error("Scenario :'%s' at step: '%s' failed! ", scenario.name, step.name)
            logging.debug("Expected result: %s", tck_util.as_cypger_text(context.expected))
            logging.debug("Actual result: %s", tck_util.as_cypger_text(context.results))
        if step.status == 'skipped':
            logging.warn("Scenario :'%s' at step: '%s' was skipped! ", scenario.name, step.name)
        if step.status == 'passed':
            logging.debug("Scenario :'%s' at step: '%s' was passed! ", scenario.name, step.name)
