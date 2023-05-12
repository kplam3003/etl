
from jinja2 import Environment, PackageLoader, select_autoescape
import os

env = Environment(
    loader=PackageLoader('src', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

variables = {
  **os.environ,
  'N_TRANSLATORS': int(os.environ.get('N_TRANSLATORS', 1)),
  'N_NLPERS': int(os.environ.get('N_NLPERS', 1)),
  'N_AFTER_TASKS': int(os.environ.get('N_AFTER_TASKS', 2))
}

worker_template = env.get_template('worker/docker-compose.yml.jinja')
worker_template.stream(**variables).dump('.build/worker/docker-compose.yml')

worker_env_template = env.get_template('worker/.env.jinja')
worker_env_template.stream(**variables).dump('.build/worker/.env')

# orchestrator_template = env.get_template('orchestrator/docker-compose.yml.jinja')
# orchestrator_template.stream(**variables).dump('.build/orchestrator/docker-compose.yml')
#
# orchestrator_env_template = env.get_template('orchestrator/.env.jinja')
# orchestrator_env_template.stream(**variables).dump('.build/orchestrator/.env')

web_sync = env.get_template('web_sync/docker-compose.yml.jinja')
web_sync.stream(**variables).dump('.build/web_sync/docker-compose.yml')

web_sync = env.get_template('web_sync/.env.jinja')
web_sync.stream(**variables).dump('.build/web_sync/.env')

case_study_orchestrator = env.get_template('case_study_orchestrator/docker-compose.yml.jinja')
case_study_orchestrator.stream(**variables).dump('.build/case_study_orchestrator/docker-compose.yml')

case_study_orchestrator = env.get_template('case_study_orchestrator/.env.jinja')
case_study_orchestrator.stream(**variables).dump('.build/case_study_orchestrator/.env')

company_datasource_orchestrator = env.get_template('company_datasource_orchestrator/docker-compose.yml.jinja')
company_datasource_orchestrator.stream(**variables).dump('.build/company_datasource_orchestrator/docker-compose.yml')

company_datasource_orchestrator = env.get_template('company_datasource_orchestrator/.env.jinja')
company_datasource_orchestrator.stream(**variables).dump('.build/company_datasource_orchestrator/.env')

migration_env_template = env.get_template('migration/.env.jinja')
migration_env_template.stream(**variables).dump('.build/migration/.env')

migration_template = env.get_template('migration/docker-compose.yml.jinja')
migration_template.stream(**variables).dump('.build/migration/docker-compose.yml')

luminati_template = env.get_template('proxy/docker-compose.yml.jinja')
luminati_template.stream(**variables).dump('.build/proxy/docker-compose.yml')

webhook_env_template = env.get_template('webhook/.env.jinja')
webhook_env_template.stream(**variables).dump('.build/webhook/.env')

webhook_template = env.get_template('webhook/docker-compose.yml.jinja')
webhook_template.stream(**variables).dump('.build/webhook/docker-compose.yml')
